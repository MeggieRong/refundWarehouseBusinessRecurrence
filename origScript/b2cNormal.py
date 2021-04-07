from time import strftime, localtime
import pandas as pd
import numpy as np
import tqdm
import warnings
from sqlalchemy import create_engine
import pymysql
import datetime
from copy import deepcopy
from operator import itemgetter
from itertools import groupby
import pyarrow.feather as feather
# from memory_profiler import profile
from itertools import chain
warnings.filterwarnings("ignore")


# @profile
# def main_func():
    # 输入要遍历的数据源日期.确保日期范围可以包容包括出库和入库的所有日期范围

startDate = '2020-01-10'
endDate = '2020-01-10'

# 选择是否要进行冷sku移库操作（Y/N）,以及冷sku的间隔天数（如果不要这个功能，则填写啥都无所谓）
# TODO ADD OPTION NO
ifUseRemoveColdSKU, coldRangeTime = 'Y', 7

# 选择是否要进行理库合并零散箱子的操作（Y/N）
# TODO ADD OPTION NO
ifUseTally = 'Y'

# 选择是否要在refund入库的时候，进行初次理箱，那个箱子最多装x件（Y/N）
# TODO ADD OPTION NO
ifUseFirstTally, max_qty = 'Y', 15


class Inventory:
    def __init__(self, inv):
        self.inv = inv
        self.sku_to_inv = {}

    def set_inv(self, inv):
        self.inv = inv

    def build_sku_dict(self):
        for item in self.inv:
            if not self.sku_to_inv.__contains__(item["sku"]):
                self.sku_to_inv[item["sku"]] = []
            self.sku_to_inv[item["sku"]].append(item)

    def get_sku_inv(self, sku):
        return self.sku_to_inv[sku]


# ==================超过15箱的拆出来，小于15件的合并，总之尽可能每个箱子都是15件
# 入库的时候先做箱子分割 : step1-按照时间和sku件数升序
def count_qty_in_array(arr):
    sum = 0
    for i in arr:
        sum += i["qty"]
    return sum


def addNewBinNo_(arr,num):
    n = num
    tmp = []
    for item in arr:
        for i in item:
            if type(i) is str:
                pass
            else:
                i['bin_no'] = n
        n += 1
        tmp.append(item)
    tmp2 = []
    for item in tmp:
        for j in item:
            tmp2.append(j)
    arr = tmp2.copy()
    return arr


def refundFirstTally(refund,bin_num):
    # 先把超过15件的拆了，留下整15件和零散件数的
    new_refund = []
    # tmp_refund = []
    for item in refund:
        # 本身的件数是否超过了max_qty
        if item["qty"] >= max_qty:
            # 那么就要每次减少15，并且在新的箱子单独放15件该sku
            while item['qty'] > 0:
                # 如果还是等于大于15件，那么直接给与15件的件数，然后件数要减少max_qty
                tmp_refund = {}
                if item['qty'] >= max_qty:
                    tmp_refund['date'] = item['date']
                    tmp_refund['sku'] = item['sku']
                    tmp_refund['qty'] = max_qty
                    item['qty'] -= max_qty
                    new_refund.append(tmp_refund)
                else:
                    # 如果已经减少到小于max_qty，那么就另外放一个临时箱子了
                    tmp_refund['date'] = item['date']
                    tmp_refund['sku'] = item['sku']
                    tmp_refund['qty'] = item['qty']
                    item['qty'] = 0
                    new_refund.append(tmp_refund)
        else:
            new_refund.append({
                'date': item['date'],
                'sku': item['sku'],
                'qty': item['qty']
            })

    new_refund = sorted(new_refund, key=lambda i: i['qty'], reverse=True)

    # 再把现在都不会超过15件的整箱和零散库存，合并也不能超过15件
    new_refund_C = []
    tmp_C = []
    for item in new_refund:
        # 如果已经到达15件，则单独一个箱子
        if item["qty"] == max_qty:
            new_refund_C.append([deepcopy(item)])
            item["qty"] = 0
        else:
            while item["qty"] != 0 or still_need_qty != 0:
                still_need_qty = 0
                if item["qty"] + count_qty_in_array(tmp_C) < max_qty:
                    tmp_C.append(deepcopy(item))
                    item["qty"] = 0
                # 如果加入后超过了15件，那么考虑下只需要加入那几件就好了
                else:
                    still_need_qty = max_qty - count_qty_in_array(tmp_C)
                    if still_need_qty != 0 and still_need_qty <= item["qty"]:
                        tmp_C.append({
                            'date': item['date'],
                            'sku': item['sku'],
                            'qty': still_need_qty
                        })
                        new_refund_C.append(tmp_C)
                        # 已经收到箱子里面了，需要另外开一个箱子
                        if item["qty"] == 0:
                            tmp_C = [item]
                        else:
                            item["qty"] = item["qty"] - still_need_qty
                    else:
                        tmp_C = [item]
    _bin_num = bin_num
    new_refund_C.append(tmp_C)
    new_refund = addNewBinNo_(new_refund_C,bin_num)
    new_refund = list(filter(lambda x: x['qty'] != 0, new_refund))
    refund = new_refund.copy()
    return refund


grouper = itemgetter("sku")
def getSkuList(list):
    sku_list = []
    for key, grp in groupby(list, grouper):
        sku_list.append(key)
    return sku_list


# 建立数据库连接，准备存储结果
db = pymysql.connect(host='172.20.8.216',
                     user="root",
                     password="root",
                     database="JY")
cursor = db.cursor()
pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
engine = create_engine(
    'mysql+pymysql://root:root@172.20.8.216:3306/JY', encoding='utf8')

# sql1 = '''
# CREATE TABLE `kubot_log`.`part_sale_non_sku2`  (
#   `index` varchar(255) NULL,
#   `date` varchar(255) NULL,
#   `order` varchar(255) NULL,
#   `sku` varchar(255) NULL,
#   `qty` varchar(255) NULL,
#   `order_type` varchar(255) NULL
# );
# '''
# cursor.execute(sql1)

# 初始库存
inv = []
inventory = Inventory(inv)
remove_cold_sku_inv = []


all_orders = feather.read_feather('all_orders.feather')
all_refund = feather.read_feather('all_refund.feather')


# 遍历每一天的sale和refund
for date in tqdm.tqdm(pd.date_range(start=startDate, end=endDate)):

    # 退货仓出库订单行
    refund_sale = pd.DataFrame()

    # 退货仓结余库存明细
    refund_inv = pd.DataFrame()

    # 冷sku移库明细
    remove_cold_inv = pd.DataFrame()

    # 理库前后箱子去重计数对比表
    Tabbly_before_after = pd.DataFrame()
    order = all_orders[all_orders["date"] == date]
    nowadays_refund = all_refund[all_refund["date"] == date]

    nowadays_refund = nowadays_refund.to_dict(orient='records')
    nowadays_refund = sorted(nowadays_refund, key=lambda i: (i['qty']), reverse=False)

    # 当天的inv要进行入库的理货处理
    if len(inv) == 0 :
        last_bin_num = 1
    else:
        #要找到最大的bin_no
        last_bin_num = [max(item['bin_no'] for item in inv)][0]
    nowadays_refund = refundFirstTally(nowadays_refund,last_bin_num)

    def countSkuNum(arr):
        global SKuCount
        countTemp = {}
        for d in arr:
            for k, v in d.items():
                countTemp.setdefault(k, set()).add(v)
                if 'sku' in countTemp.keys():
                    SKuCount = (len(countTemp['sku']))
        return SKuCount

    def countBinNum(arr):
        global binCount
        countTemp = {}
        for d in arr:
            for k, v in d.items():
                countTemp.setdefault(k, set()).add(v)
                if 'bin_no' in countTemp.keys():
                    binCount = (len(countTemp['bin_no']))
        return binCount

    def countOrder(arr):
        global OrderCount
        countTemp = {}
        for d in arr:
            for k, v in d.items():
                countTemp.setdefault(k, set()).add(v)
                if 'order' in countTemp.keys():
                    OrderCount = (len(countTemp['order']))
        return OrderCount

    # 每日入库件数
    inbound_qty = sum([item['qty'] for item in nowadays_refund])
    # 入库SKU数
    inbound_sku_count = countSkuNum(nowadays_refund)
    # #入库箱数
    inbound_bin_num = countBinNum(nowadays_refund)

    a = [item['sku'] for item in inv]
    # 如果要做理库功能，则为每一个sku添加首次入库的日期
    if ifUseRemoveColdSKU == 'Y':
        for item in nowadays_refund:
            # 如果今天退货的sku已经在库存中存在了，那么它的首次入库时间在之前已经登记过了
            if item['sku'] in a:
                item['first_inbound_date'] = [v['first_inbound_date']
                                              for v in inv if v['sku'] == item['sku']][0]
                pass
            # 如果是今天新入库的sku，那么首次入库的时间就是今天
            else:
                item.update({"first_inbound_date": date})
    else:
        pass

    # 每一天的退货都添加到昨天的库存，结合后当做今天的初始库存
    if len(nowadays_refund) > 0:
        inv = inv + nowadays_refund
        inventory.set_inv(inv)
    else:
        pass


    # 先把库存没有sku的订单行过滤，然后只遍历有sku的订单行
    order_full = order.copy()
    order = order_full[order_full.sku.isin(getSkuList(inv))]
    order = order.reset_index()
    cannot_order = order_full[~order_full.sku.isin(getSkuList(inv))]
    # cannot_order.to_sql(name='part_sale_non_sku',con=engine, if_exists='append', chunksize=1000, index=None)


    # 遍历每一个订单行
    inventory.build_sku_dict()
    takeRowsOrder_tmp_list = []
    for index, row in order.iterrows():
        print(row)

        # 本订单行需要出库的件数
        needQty = row['qty']

        # 本订单行需要出库的sku
        needSKU = row['sku']

        takenRowindex = 0
        takeRowsInv = []
        takeRowsOrder = []

        # 需要把每一个订单行的件数一直用库存循环匹配到满足了订单需要的件数needQty
        while needQty > 0:
            # 订单行需要的sku在库存的现况
            invSku = inventory.get_sku_inv(row['sku'])
            # invSku = list(filter(lambda x: x['sku'] == row['sku'], inv))

            # 如果库存没有订单所需的sku品种，则不考虑该sku.
            if not invSku:
                break

            # 如果订单行需要的件数 > 第n个箱子里sku的件数，则直接配该sku的库存给订单行，件数是这一行库存的件数

            if needQty > invSku[takenRowindex]['qty']:
                # print(index, '订单开始配库存', strftime(
                #     '%Y-%m-%d %H:%M:%S', localtime()))
                needQty = needQty - invSku[takenRowindex]['qty']
                takeRowsOrder.append({
                    'date': row['date'],
                    'order': row['order'],
                    'sku': row['sku'],
                    'order_type': row['order_type'],
                    'bin_no': invSku[takenRowindex]['bin_no'],
                    'qty': invSku[takenRowindex]['qty']
                })

            # 如果订单行需要的件数 < 第n个箱子里sku的件数，则肯定满足，一下子就配完了，件数就是订单行件数
            else:
                takeRowsOrder.append({
                    'date': row['date'],
                    'order': row['order'],
                    'sku': row['sku'],
                    'order_type': row['order_type'],
                    'bin_no': invSku[takenRowindex]['bin_no'],
                    'qty': needQty
                })
                needQty = 0

            # 更新当前所需要的sku的实时库存
            for eachLine in invSku:
                if eachLine['sku'] == row['sku'] and eachLine['bin_no'] == invSku[takenRowindex]['bin_no']:
                    eachLine['qty'] = eachLine['qty'] - \
                                      takeRowsOrder[-1]['qty']
            if takenRowindex + 1 >= len(invSku):
                break
            else:
                # 上述if通过后，如果还是订单行件数>0，那么要继续遍历该sku的下一个箱子的情况
                takenRowindex += 1

        # 退户仓销售订单行明细

        # non_fund_sale = []
        # if len(takeRowsOrder) == 0:
        #     non_fund_sale = non_fund_sale.append(row.to_dict())
        # elif row['qty'] > takeRowsOrder[0]['qty']:
        #     non_fund_sale = non_fund_sale.append(row.to_dict())
        # else:
        #     pass

        #退货仓销售
        takeRowsOrder_tmp_list.append(takeRowsOrder[0])

        # #非退货仓
        # cursor.executemany("""
        #     INSERT INTO part_sale_non_sku2 (index,date,order,sku,qty,order_type)
        #     VALUES (%(index),%(date),%(order),%(sku),%(qty),%(order_type))""", non_fund_sale)
        # db.commit()



    # 正常的每天的结余库存
    for item in inv:
        item['record_date'] = date


    # 移库库功能
    _tmp = list(order[order['date'] == date]['sku'])
    if ifUseRemoveColdSKU == 'Y':
        for item in inv:
            # 如果sku在今天有出库过，就不移库，然后更新最近一次出库的日期就是今天
            if item['sku'] in _tmp:
                item['last_outbound_date'] = date

            # 如果今天不出库，以前出库过，则保留以前的出库日期
            elif 'last_outbound_date' in item:
                if item['last_outbound_date'] != 'null':
                    # 场景1：如果有过出库，那么最近一次出库的日期 + 冷sku时间间隔 = 今天，则在今天要移除
                    if item['last_outbound_date'] + datetime.timedelta(days=coldRangeTime) == date:
                        # 登记要移库的明细
                        remove_cold_sku_inv.append({
                            'remove_date': date,
                            'sku': item['sku'],
                            'bin_no': item['bin_no'],
                            'qty': item['qty'],
                            'first_inbound_date': item['first_inbound_date'],
                            'last_outbound_date': item['last_outbound_date']})
                        # 然后直接把件数设置为0，当做已经移库
                        item['qty'] = 0
                else:
                    # 场景2:如果一直以来都没有出库，那么首次入库 + 冷sku时间间隔 = 今天，则在今天要移除
                    if item['first_inbound_date'] + datetime.timedelta(days=coldRangeTime) == date:
                        # 登记要移库的明细
                        remove_cold_sku_inv.append({
                            'remove_date': date,
                            'sku': item['sku'],
                            'bin_no': item['bin_no'],
                            'qty': item['qty'],
                            'first_inbound_date': item['first_inbound_date'],
                            'last_outbound_date': item['last_outbound_date']})
                        # 然后直接把件数设置为0，当做已经移库
                        item['qty'] = 0

            else:
                # 如果今天不出库，以前也没有出库过，则出库日期为空
                item['last_outbound_date'] = 'null'

    # 更新库存，删减掉移库的明细
    inv = list(filter(lambda x: x['qty'] != 0, inv))
    inventory.set_inv(inv)

    # #保留每个sku的first_inbound_date
    # generator = map(itemgetter('value','first_inbound_date'), test_data)

    # 每日结余库存-包含已经入库预处理 + 销售 + 移除冷sku
    # refund_inv = refund_inv.append(inv)

    # 理货功能
    # 理货前后的箱子数对比表
    TallyBinVsTmp = []
    TallyBinVsTmp7 = []

    if ifUseTally == 'Y':
        # 进行理货功能之前，要先统计一下当前库存箱子数
        def countBinNum(arr):
            countTemp = {}
            for d in arr:
                for k, v in d.items():
                    countTemp.setdefault(k, set()).add(v)
                    if 'bin_no' in countTemp.keys():
                        binCount = (len(countTemp['bin_no']))
            return binCount
        # 获取单箱件数低于7的库存明细
        inv_less_than_7 = list(filter(lambda x: x['qty'] <= 7, inv))

        # 低于7件的箱子数
        binCount7 = countBinNum(inv_less_than_7)

        # 高于7件件的箱子数
        binCount7_ = countBinNum(inv) - binCount7


        # 所有的库存都按照件数升序，新创建一个仓库，
        # 假若数值小于10，则可以加入然后一直加够15，则给一个新的箱子编号。如果大于10就不要加了
        inv = sorted(inv, key=lambda i: i['qty'], reverse=False)
        inventory.set_inv(inv)

        # 累加map的件数
        def sum_qty_in_array(arr):
            sum = 0
            for i in arr:
                sum += i['qty']
            return sum

        new_inv = []
        tmp = []
        for item in inv:
            if item['qty'] + sum_qty_in_array(tmp) < max_qty:
                tmp.append(item)
            else:
                # 如果和当下的tmp相加后的件数大于15了，那么该item不能添加到tmp，而是先把当前的tmp放好
                # 放到new_inv后，清空tmp为当下的item，那就重新开一个箱子了
                new_inv.append(deepcopy(tmp))

                tmp = [item]
        # new_inv.append(deepcopy(tmp))
        new_inv.append(tmp)

        # 给所有箱子从1开始编号
        def addNewBinNo(arr):
            n = 1
            tmp = []
            for item in arr:
                for i in item:
                    i['bin_no'] = n
                n += 1
                tmp.append(item)
            tmp2 = []
            for item in tmp:
                for j in item:
                    tmp2.append(j)
            arr = tmp2.copy()
            return arr

        # 得到新的理货后的库存
        # inv = deepcopy(addNewBinNo(new_inv))
        inv = addNewBinNo(new_inv)
        inventory.set_inv(inv)

        # 把理货后的库存总箱子数箱子数登记起来
        binCount = countBinNum(inv) - binCount7_

    refund_inv = refund_inv.append(inv)

    insert_db_inv = pd.DataFrame()
    # 结余库存件数、结余SKU数、结余库存箱数
    inv_left_qty = refund_inv.qty.sum()
    inv_left_sku = len(set(refund_inv.sku))
    inv_left_binNum = len(set(refund_inv.bin_no))

    # 每个sku的箱子数除以sku品种数，再总体平均
    refund_inv_sku_detail = pd.pivot_table(refund_inv, index=['sku'], values=[
        'bin_no'], aggfunc='count')
    refund_inv_sku_detail = refund_inv_sku_detail.reset_index()
    binNumEachBin = refund_inv_sku_detail.bin_no.mean()
    # refund_inv_sku_detail['avgBinNumEachBin'] = refund_inv_sku_detail.bin_no / inv_left_sku
    # binNumEachBin = refund_inv_sku_detail.avgBinNumEachBin.mean()

    # 每SKU每箱平均件数（每个sku各自总件数除以总箱子数，再统一平均）
    refund_inv_sku_detail2 = pd.pivot_table(
        refund_inv, index=['sku'], values=['qty','bin_no'], aggfunc={'qty' :np.sum,'bin_no':'count'})
    refund_inv_sku_detail2 = refund_inv_sku_detail2.reset_index()


    refund_inv_sku_detail2['avgQtyEachBin'] = refund_inv_sku_detail2.qty / refund_inv_sku_detail2.bin_no
    avgQtyEachBin = refund_inv_sku_detail2.avgQtyEachBin.mean()

    # 冷sku移库明细表
    remove_cold_inv = remove_cold_inv.append(remove_cold_sku_inv)


    if len(remove_cold_inv) != 0:
        # 下架sku数量
        cold_drop_sku = len(set(remove_cold_inv.sku))
        # 下降箱子数
        cold_drop_binNum = len(set(remove_cold_inv.bin_no))
        # 平均每sku下架件数
        drop_cold_sku_qty = pd.pivot_table(
            remove_cold_inv, index=['sku'], values=['qty'], aggfunc=np.sum)
        drop_cold_sku_qty = drop_cold_sku_qty.reset_index()
        avg_drop_cold_sku_qty = drop_cold_sku_qty.qty.mean()
        # 平均每SKU下架搬箱数
        # drop_cold_sku_binNum = pd.pivot_table(remove_cold_inv, index=['sku'], values=[
        #     'bin_no'], aggfunc=pd.Series.nunique)
        drop_cold_sku_binNum = remove_cold_inv.groupby('sku').bin_no.nunique()

        drop_cold_sku_binNum = drop_cold_sku_binNum.reset_index()
        drop_cold_sku_binNum = drop_cold_sku_binNum.bin_no.mean()
    else:
        cold_drop_sku = 0
        cold_drop_binNum = 0
        avg_drop_cold_sku_qty = 0
        drop_cold_sku_binNum = 0

    #出库件数
    refund_sale_qty = sum([item['qty'] for item in takeRowsOrder_tmp_list])
    # 出库SKU数
    refund_sale_sku = countSkuNum(takeRowsOrder_tmp_list)
    # 出库行数
    refund_sale_orderLine = len(takeRowsOrder_tmp_list)
    # #出库订单数#
    refund_sale_orderCount = countOrder(takeRowsOrder_tmp_list)
    # 出库搬箱数
    refund_sale_orderCount = countBinNum(takeRowsOrder_tmp_list)



    # 把几个字段合并到一个list输出
    unionlist_df = pd.DataFrame(columns=['inbound_qty', 'inbound_sku_count', 'inbound_bin_num', 'refund_sale_qty', 'refund_sale_sku', 'refund_sale_orderLine', 'refund_sale_orderCount', 'refund_sale_orderCount',
                                         'cold_drop_sku', 'cold_drop_binNum', 'avg_drop_cold_sku_qty', 'drop_cold_sku_binNum', 'binCount7', 'binCount', 'inv_left_qty', 'inv_left_sku', 'inv_left_binNum', 'binNumEachBin', 'avgQtyEachBin'])

    unionlist = [inbound_qty, inbound_sku_count, inbound_bin_num, refund_sale_qty, refund_sale_sku, refund_sale_orderLine, refund_sale_orderCount, refund_sale_orderCount,
                 cold_drop_sku, cold_drop_binNum, avg_drop_cold_sku_qty, drop_cold_sku_binNum, binCount7, binCount, inv_left_qty, inv_left_sku, inv_left_binNum, binNumEachBin, avgQtyEachBin]

    unionlist_df.loc[len(unionlist_df)] = unionlist
    unionlist_df['date'] = date
    unionlist_df.to_sql(name='B2C_result',
                        con=engine, if_exists='append', chunksize=1000, index=None)
#     return
#
# if __name__ == "__main__":
#     main_func()