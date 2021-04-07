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
from itertools import chain
import itertools
warnings.filterwarnings("ignore")


# @profile
# def main_func():
    # 输入要遍历的数据源日期.确保日期范围可以包容包括出库和入库的所有日期范围
startDate = '2020-03-01'
endDate = '2020-05-31'

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
        tmp2 = list(itertools.chain.from_iterable(tmp))
    return tmp2

def addNewBinNo2(arr):
    n = 1
    tmp = []
    for item in arr:
        for i in item:
            i['new_bin_no'] = n
        n += 1
        tmp.append(item)
        tmp2 = list(itertools.chain.from_iterable(tmp))
    return tmp2

grouper_kuanhao = itemgetter("kuan_hao")
def getKuanHaoList(list):
    kuanhao_list = []
    for key, grp in groupby(list, grouper_kuanhao):
        kuanhao_list.append(key)
    return kuanhao_list

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
            new_refund_C.append([item.copy()])
            item["qty"] = 0
        else:
            while item["qty"] != 0 or still_need_qty != 0:
                still_need_qty = 0
                if item["qty"] + count_qty_in_array(tmp_C) < max_qty:
                    tmp_C.append(item.copy())
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

def countSkuNum(arr):
    SKuCount = 0
    countTemp = {}
    for d in arr:
        for k, v in d.items():
            countTemp.setdefault(k, set()).add(v)
            if 'sku' in countTemp.keys():
                SKuCount = (len(countTemp['sku']))
    return SKuCount

def countBinNum(arr):
    binCount = 0
    countTemp = {}
    for d in arr:
        for k, v in d.items():
            countTemp.setdefault(k, set()).add(v)
            if 'bin_no' in countTemp.keys():
                binCount = (len(countTemp['bin_no']))
    return binCount


def countOrder(arr):
    orderCount =  0
    countTemp = {}
    for d in arr:
        for k, v in d.items():
            countTemp.setdefault(k, set()).add(v)
            if 'order_no' in countTemp.keys():
                orderCount = (len(countTemp['order_no']))
    return  orderCount

# 建立数据库连接，准备存储结果
db = pymysql.connect(host='172.20.5.197',
                     user="root",
                     password="root",
                     database="kubot_log")
cursor = db.cursor()
pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
engine = create_engine(
    'mysql+pymysql://root:root@172.20.5.197:3306/kubot_log', encoding='utf8')

sql1 = '''
CREATE TABLE `kubot_log`.`part_sale_non_sku_B2B_2`  (
  `date` varchar(255) NULL,
  `order_no` varchar(255) NULL,
  `sku` varchar(255) NULL,
  `qty` varchar(255) NULL,
  `order_type` varchar(255) NULL
);
'''
cursor.execute(sql1)

# 初始库存
inv = []
inventory = Inventory(inv)



all_orders = feather.read_feather('B2B_orders.feather')
all_refund = feather.read_feather('B2B_normal_refund.feather')

# 遍历每一天的sale和refund
for date in tqdm.tqdm(pd.date_range(start=startDate, end=endDate)):
    non_fund_sale = []

    # 退货仓出库订单行
    refund_sale = pd.DataFrame()

    # 退货仓结余库存明细
    refund_inv = pd.DataFrame()
    # order = all_orders[all_orders.date == date]
    a = date.strftime('%Y-%m-%d')
    order = all_orders[all_orders["date"] == a]
    nowadays_refund = all_refund[all_refund.date == date]
    nowadays_refund = nowadays_refund.to_dict(orient='records')
    nowadays_refund = sorted(nowadays_refund, key=lambda i: (i['qty']), reverse=False)

    # 当天的inv要进行入库的理货处理
    if len(inv) == 0 :
        last_bin_num = 1
    else:
        #要找到最大的bin_no
        last_bin_num = [max(item['bin_no'] for item in inv)][0]
    nowadays_refund = refundFirstTally(nowadays_refund,last_bin_num)

    # 每日入库件数
    inbound_qty = sum([item['qty'] for item in nowadays_refund])
    # 入库SKU数
    inbound_sku_count = countSkuNum(nowadays_refund)
    # #入库箱数
    inbound_bin_num = countBinNum(nowadays_refund)


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
    cannot_order.to_sql(name='part_sale_non_sku_B2B',con=engine, if_exists='append', chunksize=1000, index=None)


    # 遍历每一个订单行
    inventory.build_sku_dict()
    takeRowsOrder_tmp_list = []

    for index, row in order.iterrows():
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
                    'order_no': row['order_no'],
                    'sku': row['sku'],
                    'order_type': row['order_type'],
                    'bin_no': invSku[takenRowindex]['bin_no'],
                    'qty': invSku[takenRowindex]['qty']
                })
                invSku[takenRowindex]['qty'] = 0
            # 如果订单行需要的件数 < 第n个箱子里sku的件数，则肯定满足，一下子就配完了，件数就是订单行件数
            else:
                takeRowsOrder.append({
                    'date': row['date'],
                    'order_no': row['order_no'],
                    'sku': row['sku'],
                    'order_type': row['order_type'],
                    'bin_no': invSku[takenRowindex]['bin_no'],
                    'qty': needQty
                })
                invSku[takenRowindex]['qty'] = invSku[takenRowindex]['qty'] - needQty
                needQty = 0

            # 更新当前所需要的sku的实时库存
            # for eachLine in invSku:
            #     if eachLine['sku'] == row['sku'] and eachLine['bin_no'] == invSku[takenRowindex]['bin_no']:
            #         eachLine['qty'] = eachLine['qty'] - \
            #                           takeRowsOrder[-1]['qty']

            if takenRowindex + 1 >= len(invSku):
                break
            else:
                # 上述if通过后，如果还是订单行件数>0，那么要继续遍历该sku的下一个箱子的情况
                takenRowindex += 1
        # 退户仓销售订单行明细
        if len(takeRowsOrder) == 0:
            del row['index']
            non_fund_sale.append(row.to_dict())
        elif row['qty'] > takeRowsOrder[0]['qty']:
            del row['index']
            non_fund_sale.append(row.to_dict())
        else:
            pass

        #退货仓销售
        takeRowsOrder_tmp_list.append(takeRowsOrder[0])


    # #把今天的订单从总订单删除all_orders
    # all_orders = all_orders[all_orders.date != date]

    #非退货仓
    cursor.executemany("""
        INSERT INTO part_sale_non_sku_B2B_2 (date,order_no,sku,qty,order_type)
        VALUES (%(date)s,%(order_no)s,%(sku)s,%(qty)s,%(order_type)s)""", non_fund_sale)
    db.commit()

    # 正常的每天的结余库存
    for item in inv:
        item['record_date'] = date
    inventory.set_inv(inv)

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

    # 每SKU每箱平均件数（每个sku各自总件数除以总箱子数，再统一平均）
    refund_inv_sku_detail2 = pd.pivot_table(
        refund_inv, index=['sku'], values=['qty','bin_no'], aggfunc={'qty' :np.sum,'bin_no':'count'})
    refund_inv_sku_detail2 = refund_inv_sku_detail2.reset_index()


    refund_inv_sku_detail2['avgQtyEachBin'] = refund_inv_sku_detail2.qty / refund_inv_sku_detail2.bin_no
    avgQtyEachBin = refund_inv_sku_detail2.avgQtyEachBin.mean()


    #出库件数
    refund_sale_qty = sum([item['qty'] for item in takeRowsOrder_tmp_list])
    # 出库SKU数
    refund_sale_sku = countSkuNum(takeRowsOrder_tmp_list)
    # 出库行数
    refund_sale_orderLine = len(takeRowsOrder_tmp_list)
    # #出库订单数#
    refund_sale_Count = countOrder(takeRowsOrder_tmp_list)
    # 出库搬箱数
    refund_sale_orderBinNUm = countBinNum(takeRowsOrder_tmp_list)

    # ========复杂理货核心
    # 获取对象的'sku'这个位置上的数据
    grouper = itemgetter("sku")

    result = []
    for key, grp in groupby(sorted(inv, key=grouper), grouper):
        # zip(A,B) 将listA和listB合并为数组
        # 在这一步，我们获取去重后的sku明细
        temp_dict = dict(zip(["sku"], [key]))
        temp_dict["qty"] = sum(item["qty"] for item in grp)
        result.append(temp_dict)

    result = sorted(result, key=lambda i: (i['qty']), reverse=True)

    # 以下的sku因为总件数>= max_qty，所以需要一箱一sku，每箱最多max_qty，最后即便只剩下1件，也不能混箱
    singleSku = list(filter(lambda x: x['qty'] >= max_qty, result))
    mixSku = list(filter(lambda x: x['qty'] < max_qty, result))

    singleSku_list = getSkuList(singleSku)
    mixSku_list = getSkuList(mixSku)

    # sku能抽取满箱的排前面，零碎的sku排后面。库存按照这个sku排序后，再组内排序
    sku_sort = singleSku_list + mixSku_list

    # inv先根据sku的总件数升序，然后再sku组内升序
    inv = sorted(inv, key=lambda i: i['qty'], reverse=True)
    inv.sort(key=lambda x: sku_sort.index(x["sku"]))

    # 正式开始
    # print('开始复杂理箱', strftime('%Y-%m-%d %H:%M:%S', localtime()))
    tmp = []
    dest = []
    # print('要遍历的箱子数是', len(inv), strftime('%Y-%m-%d %H:%M:%S', localtime()))
    for item in inv:
        while item['qty'] != 0:
            # print(1, item)
            if len(tmp) == 0:
                # print(2)
                tmp.append(item.copy())
                item['qty'] = 0
            elif item['qty'] == max_qty:
                dest.append([item.copy()])
                item['qty'] = 0
            elif (item['sku'] in singleSku_list) and (item['sku'] in getSkuList(tmp)) \
                    and (item['qty'] + count_qty_in_array(tmp) < max_qty):
                # print(3)
                tmp.append(item.copy())
                item['qty'] = 0
            elif (item['sku'] in singleSku_list) and (item['sku'] in getSkuList(tmp)) \
                    and (item['qty'] + count_qty_in_array(tmp) == max_qty):
                # print(4)
                tmp.append(item.copy())
                dest.append(tmp)
                item['qty'] = 0
                tmp = [item]
            elif (item['sku'] in singleSku_list) and (item['sku'] in getSkuList(tmp)) \
                    and (item['qty'] + count_qty_in_array(tmp) > max_qty):
                # print(5, item)
                still_need_qty = max_qty - count_qty_in_array(tmp)
                tmp.append({
                    'date': item['date'],
                    'sku': item['sku'],
                    'bin_no': item['bin_no'],
                    'qty': still_need_qty
                })
                dest.append(tmp)
                item['qty'] = item['qty'] - still_need_qty
                tmp = [item]
                # print('5 tmp is: ', tmp)
            elif (item['sku'] in singleSku_list) and (item['sku'] not in getSkuList(tmp)):
                # print(6)
                dest.append(tmp)
                tmp = [item.copy()]
                item['qty'] = 0
            elif (item['sku'] in singleSku_list) and (item['sku'] not in getSkuList(tmp)):
                # print(7)
                tmp.append(item.copy())
                dest.append(tmp)
                item['qty'] = 0
            elif (item['sku'] not in singleSku_list):
                # print(8)
                if (item['sku'] not in getSkuList(tmp)) and (getSkuList(tmp)[0] in singleSku_list):
                    # print(9)
                    dest.append(tmp)
                    tmp = []
                else:
                    # print(10)
                    pass
                while item['qty'] != 0:
                    # print(11)
                    if item['qty'] + count_qty_in_array(tmp) <= max_qty:
                        # print(12)
                        tmp.append(item.copy())
                        item['qty'] = 0
                    elif item['qty'] + count_qty_in_array(tmp) > max_qty:
                        # print(13)
                        need_qty = max_qty - count_qty_in_array(tmp)
                        tmp.append({
                            'date': item['date'],
                            'sku': item['sku'],
                            'bin_no': item['bin_no'],
                            'qty': need_qty
                        })
                        dest.append(tmp)
                        item['qty'] = item['qty'] - need_qty
                        tmp = [item]
                    else:
                        # print(14)
                        dest.append(tmp)
                        tmp = []
            else:
                # print(15)
                pass
    dest.append(tmp)

    dest = addNewBinNo2(dest)

    dest = list(filter(lambda x: x['qty'] != 0, dest))
    inv_after_complex_tally = pd.DataFrame(dest)
    # print('结束复杂理箱', strftime('%Y-%m-%d %H:%M:%S', localtime()))
    # 每日复杂理箱的动碰箱
    # inv_after_complex_tally.to_sql(name='B2B_normal_complexTally_inv_everyday',
    #                                con=engine, if_exists='append', chunksize=1000, index=None)

    #库存达到15件的sku数
    qty_over_sku_count_df = inv_after_complex_tally.groupby('sku').qty.sum()
    inv_after_complex_tally.reset_index(inplace = True,drop = True)
    remove_list = inv_after_complex_tally[inv_after_complex_tally['qty'] >= 15].sku.tolist()
    qty_over_sku_count = inv_after_complex_tally[inv_after_complex_tally['qty'] >= 15].sku.count()

    #理货搬出箱数
    qty_over_bin_count = inv_after_complex_tally[inv_after_complex_tally['sku'].isin(remove_list)].bin_no.nunique()

    #平均每理sku所占箱子数
    avg_sku_bin_df = inv_after_complex_tally[inv_after_complex_tally['sku'].isin(remove_list)].groupby('sku').bin_no.nunique()
    avg_sku_bin = avg_sku_bin_df.mean()

    #平均每理货sku件数
    avg_sku_qty_df = inv_after_complex_tally[inv_after_complex_tally['sku'].isin(remove_list)].groupby('sku').qty.sum()
    avg_sku_qty = avg_sku_qty_df.mean()

    #理货后回库箱数
    qty_over_bin_count_return = inv_after_complex_tally[inv_after_complex_tally['sku'].isin(remove_list)].new_bin_no.nunique()

    # 每日复杂理箱后的库存
    keys_to_keep = ['date', 'sku', 'qty', 'new_bin_no']
    dest = [{key: item[key] for key in keys_to_keep} for item in dest]
    for d in dest:
        d['bin_no'] = d.pop('new_bin_no')

    inv = dest.copy()

    # # 同款不同色的冻结下架
    # if date.strftime("%Y-%m-%d") in ['2020-05-31', '2020-08-31', '2020-11-30']:
    #     inv = []
    # else:
    #     pass

    #把几个字段合并到一个list输出
    unionlist_df = pd.DataFrame(columns=['inbound_qty',
                                         'inbound_sku_count',
                                         'inbound_bin_num',
                                         'refund_sale_qty',
                                         'refund_sale_sku',
                                         'refund_sale_orderLine',
                                         'refund_sale_Count',
                                         'refund_sale_orderBinNUm',
                                         'qty_over_sku_count',
                                         'qty_over_bin_count',
                                         'avg_sku_bin',
                                         'avg_sku_qty',
                                         'qty_over_bin_count_return',
                                         'inv_left_qty',
                                         'inv_left_sku',
                                         'inv_left_binNum',
                                         'binNumEachBin',
                                         'avgQtyEachBin'])

    unionlist = [inbound_qty,
                 inbound_sku_count,
                 inbound_bin_num,
                 refund_sale_qty,
                 refund_sale_sku,
                 refund_sale_orderLine,
                 refund_sale_Count,
                 refund_sale_orderBinNUm,
                 qty_over_sku_count,
                 qty_over_bin_count,
                 avg_sku_bin,
                 avg_sku_qty,
                 qty_over_bin_count_return,
                 inv_left_qty,
                 inv_left_sku,
                 inv_left_binNum,
                 binNumEachBin,
                 avgQtyEachBin]

    unionlist_df.loc[len(unionlist_df)] = unionlist
    unionlist_df['date'] = date
    unionlist_df.to_sql(name='B2B_normal_result',
                        con=engine, if_exists='append', chunksize=1000, index=None)

#     return
#
# if __name__ == "__main__":
#     main_func()