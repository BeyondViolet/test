import asyncio
import datetime
import logging
import traceback

import requests

from core.functions import getBarcodesLenta, add_sku_barcode
from faker import Faker

competitor_name = 'лента'
store_id = 592  # ID магазина у store_code = 0088 Гипермаркет из новосибирска Большевистская д.52/1
store_codes = ['0073', '0088']  # Количество SKU больше всех у store_code = 0073

API_URL = "https://lenta.com/api/v1/stores/{store_code}/skus/?barcode={barcode}"
fake = Faker()


# ПОЯСНИТЕЛЬНАЯ БРИГАДА:
# Загрузка осуществляется раз в неделю сопоставления штрихкодов ламы и ЛЕНТЫ у ВСЕГО одного магазина, просто обновляем СКУ
# В НАШЕМ СЛУЧАЕ ЭТО Гипермаркет из новосибирска Большевистская д.52/1
# ЗАПИСЫВАЕТСЯ В ТАБЛИЦУ competitor.product_sku и больше никуда
# Сканирование в дальнейшем осуществляется с помощью алгоритма из файла lenta.py, которая загружает списками SKU магазина лента
# А т.к. мы знаем SKU из предыдущего сканирования магазина с БОЛЬШЕВИТСКОЙ, то мы смело достаем нужные нам SKU / ШТРИХКОДЫ
# Делаем сопоставления и сохраняем к себе в базу!
# ЗА ЧТО НАС БАНЯТ:
# 1. Частые запросы, делаем раз в 2 секунды -- await asyncio.sleep(2)
# 2. Частая смена user-agent

async def lenta_get_sku_barcodes():
    barcodes = await getBarcodesLenta()
    logging.info(f"Общее количество НЕСОПОСТАВЛЕННЫХ SKU: {len(barcodes)}")
    i = 1
    rows = []
    rows_is_check = []
    API_HEADER = {'user-agent': fake.user_agent()}

    for store_code in store_codes:
     for barcode in barcodes:

        t1 = datetime.datetime.now()
        barcode = barcode['barcode']
        rows_is_check.append([store_id, barcode])
        try:
            # barcode = '016229906443'
            res = requests.get(url=API_URL.format(store_code=store_code, barcode=barcode), headers=API_HEADER)
            
            ts = (datetime.datetime.now() - t1).microseconds * 0.001
            logging.info(f"[{competitor_name}] {i} из {len(barcodes)} {API_URL.format(store_code=store_code, barcode=barcode)}: {res.status_code}, {ts} ms")
            data = res.json()
#            bv_logger.info(data)
            if data.get('code', None):
#                bv_logger.info(f"{competitor_name} {int(data['code'])} {barcode}")
                rows.append([competitor_name, int(data['code']), barcode])
        except KeyboardInterrupt:
            break
        except:
            traceback.print_exc()
            pass
        finally:
            await asyncio.sleep(1)
            # + random.random() / 4)
            #bv_logger.info(
            #    f"{i} из {len(barcodes)} / ШК {barcode} / {(datetime.datetime.now() - t1).microseconds * 0.001} ms")
            i += 1
        if i % 100 == 0:
            await add_sku_barcode(rows)
            rows = []
            # API_HEADER = {'user-agent': fake.user_agent()}
     await asyncio.sleep(1200)  # ОЖИДАЕМ 20 минут ЧАСА, ЧТОБЫ ЛЕНТА НЕ ЗАБАНИЛА ИЗ ЗА ИЗМЕНЕНИЯ USER_AGENT-а


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(lenta_get_sku_barcodes())
