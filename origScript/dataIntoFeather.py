import pyarrow.feather as feather
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine

# 建立数据库连接，准备存储结果
db = pymysql.connect(host='172.20.8.216',
                     user="root",
                     password="root",
                     database="JY")
cursor = db.cursor()
pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
engine = create_engine(
    'mysql+pymysql://root:root@172.20.8.216:3306/JY', encoding='utf8')

# searchAllOrders = """
#               SELECT
#                 date( `订单日期` ) AS date,
#                 `ORDERKEY` AS 'order_no',
#                 `货品` AS sku,
#                 sum( `出库件数` ) AS qty,
#                 `业务类型` AS order_type
#               FROM
#                 `12_months_sale`
#               GROUP BY
#                 date( `订单日期` ),
#                 `ORDERKEY`,
#                 `货品`,
#                 `业务类型`
#     """
# all_orders = pd.read_sql(searchAllOrders, con=engine)
# feather.write_feather(all_orders, 'all_orders.feather')


searchAllRefund = """SELECT
	date( `退货日期` ) AS date,
	`SKU编码` AS sku,
	888 AS bin_no,
	sum( `退货数量` ) AS qty,
	'B2C' AS sku_type 
FROM
	`B2C_normal_refund` 
GROUP BY
	date( `退货日期` ),
	`SKU编码` """
all_refund = pd.read_sql(searchAllRefund, con=engine)
feather.write_feather(all_refund, 'all_refund.feather')