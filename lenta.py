import asyncio
import datetime
import random
import traceback

import aiohttp
from bvcore.log import bv_logger
from faker import Faker
from core.functions import getStores, getSKU, addProduct

competitor_name = 'лента'
step = 1000

API_URL = "https://lenta.com/api/v1/stores/{store_code}/skus/"

fake = Faker()


async def lenta():
    stores = await getStores(competitor_name)  # Получаем все магазины ленты
    skus = await getSKU(competitor_name)  # Получаем все СКУ ленты
    cancel = False
    API_HEADER = {
        'user-agent': fake.user_agent(),
        'Host': 'lenta.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Cache-Control': 'no-cache'
    }

    for store in stores:
        store_id = store['id']
        store_code = store['store_code']
        bv_logger.info(
            f"Проверяю магазин: {store_id} Код магазина: {store_code} ССЫЛКА: {API_URL.format(store_code=store_code)}")
        offset = 0
        total = 0
        repeat_cnt = 5
        while offset >= 0:
            rows = []
            next = False
            try:
                t1 = datetime.datetime.now()
                try:
                    JSON = {'limit': step, 'offset': offset}
                    bv_logger.info(f"JSON: {JSON}")

                    async with aiohttp.ClientSession(headers=API_HEADER) as session:
                        async with session.post(API_URL.format(store_code=store_code), json=JSON) as resp:

                            if resp.status == 200:
                                products = await resp.json()
                            else:
                                products = None
                                bv_logger.info(await resp.text())

                except:
                    bv_logger.info(traceback.format_exc())
                    cancel = True
                    break
                if products and products['skus'] != []:
                    next = True
                    if offset == 0:
                        total = products['total']
                    bv_logger.info(f"{offset+step} of {products['total']}")
                    for product in products['skus']:
                        if product['regularPrice'] > 0:
                            for sku in skus:
                                if int(product['code']) == sku['sku']:
                                    row = [store_id, int(product['code']), product['title'], sku['barcode'],
                                           product['regularPrice'],
                                           product['discountPrice'],
                                           product['webUrl']]
                                    rows.append(row)
                                    # bv_logger.info(row)
                else:
                    next = False
                    repeat_cnt -= 1
                    bv_logger.info(f"{offset + step} of {products['total']}: failed. repeat")

                    if repeat_cnt == 0:
                        break
            except KeyboardInterrupt:
                cancel = True
                break
            except:
                bv_logger.info(traceback.format_exc())
                break
            finally:
                if not cancel:
                    await asyncio.sleep(5 + random.random() / 4)
                    if rows != []:
                        await addProduct(rows)
                    bv_logger.info(
                        f"Запрос выполнялся и запись в базу выполнилась успешно {(datetime.datetime.now() - t1).microseconds * 0.001} ms")

                    if next:
                        repeat_cnt = 5
                        offset = offset + step
                    if offset > total:
                        break
            if cancel:
                break
        if cancel:
            break


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(lenta())
