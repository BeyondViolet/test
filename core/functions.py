import logging
import traceback

async def getStores(competitor_name):
    stores = [
        {'id': '1', 'store_code': '0809'},
        {'id': '2', 'store_code': '0099'}
    ]
    return stores


async def getBarcodesLenta():
    barcodes = await db().fetch("""
    select distinct barcode
    from competitor.track_barcodes
    where competitor_name = 'лента')
    order by pb.barcode""")
    return barcodes


async def getSKU(competitor_name):
    skus = await db().fetch('select * from competitor.product_sku where competitor_name = $1', competitor_name)
    return skus


async def addProduct(rows):
    try:
        logging.info("Происходит загрузка в базу данных, пожалуйста подождите!")
        await db().executemany("""
                           INSERT INTO competitor.products (store_id, product_id, product_name,
                           barcode, price_max, price_min, product_url, uuid)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,uuid_generate_v4())
                           ON CONFLICT (store_id, product_name, product_id, barcode, dt ) DO NOTHING;
                         """, rows)
        logging.info("Запись в базу данных выполнена!")
    except:
        logging.info(traceback.format_exc())


async def add_sku_barcode(rows):
    try:
        logging.info("Происходит загрузка SKU / ШТРИХКОДОВ в базу данных, пожалуйста подождите!")
        await db().executemany("""
                           INSERT INTO competitor.product_sku (competitor_name, sku, barcode)
                           VALUES ($1,$2,$3)
                           ON CONFLICT (competitor_name, sku, barcode) DO NOTHING;
                         """, rows)
        logging.info("Запись в базу данных SKU / ШТРИХКОДОВ выполнена!")

    except:
        logging.info(traceback.format_exc())
