# -*- coding: utf-8 -*-
import os
import pandas as pd
# from datetime import timedelta
import numpy as np
import tqdm
import pymysql
from sqlalchemy import create_engine
import pyarrow.feather as feather
# import numba
# from numba import jit

class MergeSku():
    def __init__(self,
                 daily_invnt,
                 last_bin_no,
                 max_bin_qty,
                 action,
                 monitor_sku_invnt=pd.DataFrame()):
        self.invnt = pd.DataFrame()
        self.last_bin_no = -1

        # 理款
        if action == 1:
            # 初始化数值
            self.to_merge_type_bin_num = 0
            self.merged_type_bin_num = 0
            self.avg_bin_per_type_num = 0
            self.avg_qty_per_type_num = 0
            self.to_merge_bin_per_type_num = 0
            self.to_merge_sku_type_or_not(daily_invnt, last_bin_no, max_bin_qty)
        # 理sku
        else:
            # 初始化数值
            self.to_merge_bin_per_sku_num = 0
            self.to_merge_sku_num = 0
            self.to_merge_sku_bin_num = 0
            self.merged_sku_bin_num = 0
            self.avg_bin_per_sku_num = 0
            self.avg_qty_per_sku_num = 0
            self.monitor_sku_invnt = pd.DataFrame()
            self.to_merge_sku_or_not(daily_invnt, monitor_sku_invnt, last_bin_no, max_bin_qty)

    # 判断是否需要理sku
    def to_merge_sku_or_not(self, daily_invnt, monitor_sku_invnt, last_bin_no,
                            max_bin_qty):
        self.invnt = daily_invnt.copy()
        self.monitor_sku_invnt = monitor_sku_invnt.copy()
        _last_bin_no = last_bin_no
        tmp_df = self.invnt.groupby('sku').sum()
        # 找出库存低于20件的SKU
        sku_less_20 = set(tmp_df[tmp_df.qty < max_bin_qty].index)
        if sku_less_20 and not self.monitor_sku_invnt.empty:
            self.monitor_sku_invnt = self.monitor_sku_invnt[
                ~self.monitor_sku_invnt.sku.isin(sku_less_20)]
        sku_more_20 = set(tmp_df[tmp_df.qty >= max_bin_qty].index)
        # print('sku more than 20: ', sku_more_20)
        if sku_more_20:
            sku_more_20_df = pd.DataFrame(sku_more_20, columns=['sku'])
            sku_more_20_df.loc[:, 'day'] = 0
            self.monitor_sku_invnt = self.monitor_sku_invnt.append(sku_more_20_df,
                                                         ignore_index=True)
            self.monitor_sku_invnt = self.monitor_sku_invnt.groupby('sku').max()
            self.monitor_sku_invnt.reset_index(inplace=True)
        if not self.monitor_sku_invnt.empty:
            # 需合并的sku
            self.monitor_sku_invnt.loc[:, 'day'] = self.monitor_sku_invnt.day + 1
            to_merge_sku = set(self.monitor_sku_invnt[self.monitor_sku_invnt.day >= 7]['sku'])
            # print(to_merge_sku)
            if to_merge_sku:
                # 需要理货的sku的库存
                to_merge_sku_df = self.invnt[self.invnt.sku.isin(to_merge_sku)].copy()
                # 需理货的sku涉及的箱子
                to_merge_bin = set(
                    self.invnt[self.invnt.sku.isin(to_merge_sku)]['binNo'])
                to_merge_rest_invnt = self.invnt[self.invnt.binNo.isin(
                    to_merge_bin)].copy()
                to_merge_rest_invnt = to_merge_rest_invnt[
                    ~to_merge_rest_invnt.sku.isin(to_merge_sku)]
                # 输出所需数据
                self.to_merge_sku_num = len(to_merge_sku)
                self.to_merge_sku_bin_num = len(to_merge_bin)
                self.invnt = self.invnt[~self.invnt.binNo.isin(to_merge_bin)]
                
                # 理sku
                to_merge_sku_df, _last_bin_no, self.to_merge_bin_per_sku_num = self.merge_sku(
                    to_merge_sku_df, last_bin_no, max_bin_qty)
                self.invnt = self.invnt.append(to_merge_sku_df,
                                               ignore_index=True)
                self.merged_sku_bin_num = len(set(to_merge_sku_df['binNo']))
                self.avg_bin_per_sku_num = self.to_merge_bin_per_sku_num / self.to_merge_sku_num
                self.avg_qty_per_sku_num = to_merge_sku_df.qty.sum(
                ) / self.to_merge_sku_num
                self.last_bin_no = _last_bin_no
                
                # 理款
                if not to_merge_rest_invnt.empty:
                    to_merge_rest_invnt, _last_bin_no, _ = self.merge_sku_type(
                        to_merge_rest_invnt, _last_bin_no, max_bin_qty)
                    self.invnt = self.invnt.append(to_merge_rest_invnt,
                                                   ignore_index=True)
                    self.last_bin_no = _last_bin_no
                    self.merged_sku_bin_num += len(set(to_merge_rest_invnt['binNo']))

    # 理sku
    def merge_sku(self, daily_invnt_df, last_bin_no, max_bin_qty):
        _last_bin_no = last_bin_no
        _after_merge_sku_invnt_df = pd.DataFrame()
        _to_merge_bin_per_sku_num = 0
        to_merge_sku_invnt_df = daily_invnt_df.groupby(['sku', 'sku_id', 'sku_year_type', 'sku_color']).sum()
        to_merge_sku_invnt_df.reset_index(inplace=True)
        if not to_merge_sku_invnt_df.empty:
            # print(_to_merge_sku_invnt_df)
            sku_set = to_merge_sku_invnt_df['sku'].tolist()
            # 处理库存>=20件的sku
            for sku in sku_set:
                sku_invnt_df = to_merge_sku_invnt_df[
                    to_merge_sku_invnt_df.sku == sku].copy()
                _to_merge_bin_per_sku_num += len(set(daily_invnt_df[daily_invnt_df.sku.isin([sku])]['binNo']))
                # 初始化箱号
                sku_invnt_df.loc[:, 'binNo'] = -1
                sku_invnt_df, _last_bin_no, _ = self.create_bins(
                    sku_invnt_df, _last_bin_no + 1, max_bin_qty)
                _after_merge_sku_invnt_df = _after_merge_sku_invnt_df.append(
                    sku_invnt_df, ignore_index=True)
            return _after_merge_sku_invnt_df, _last_bin_no, _to_merge_bin_per_sku_num
        else:
            return daily_invnt_df, _last_bin_no, _to_merge_bin_per_sku_num

    # 逻辑：先判断是否理过,再理箱
    def to_merge_sku_type_or_not(self, daily_invnt, last_bin_no, max_bin_qty):
        self.invnt = daily_invnt.copy()
        to_merge_invnt_df = daily_invnt.groupby(['sku_id', 'binNo'])['qty'].sum()
        to_merge_invnt_df = pd.DataFrame(to_merge_invnt_df)
        # print(to_merge_invnt_df)
        # 找出不需要理的箱子并剔除
        to_merge_invnt_index = to_merge_invnt_df[
            to_merge_invnt_df.qty < max_bin_qty].index.tolist()
        if len(to_merge_invnt_index) > 1:
            self.invnt.set_index(['sku_id', 'binNo'], inplace=True)
            to_merge_invnt_df = self.invnt[self.invnt.index.isin(
                to_merge_invnt_index)].copy()
            to_merge_invnt_df.reset_index(inplace=True)
            self.invnt = self.invnt[~self.invnt.index.
                                            isin(to_merge_invnt_index)]
            self.invnt.reset_index(inplace=True)
            self.to_merge_type_bin_num = len(set(to_merge_invnt_df['binNo']))
            # 理款
            to_merge_invnt_df, self.last_bin_no, self.to_merge_bin_per_type_num = self.merge_sku_type(
                to_merge_invnt_df, last_bin_no, max_bin_qty)
            self.invnt = self.invnt.append(to_merge_invnt_df, ignore_index=True)
            self.merged_type_bin_num = len(set(to_merge_invnt_df['binNo']))
            self.avg_bin_per_type_num = self.to_merge_bin_per_type_num / len(
                set(self.invnt['sku']))
            self.avg_qty_per_type_num = self.invnt.qty.sum() / len(
                set(self.invnt['sku']))

    # 理款
    # 存在混箱情况. 逻辑：先判断是否理过,再理箱
    def merge_sku_type(self, to_merge_invnt_df, last_bin_no, max_bin_qty):
        _to_merge_invnt_df = to_merge_invnt_df.copy()
        _last_bin_no = last_bin_no
        _to_merge_bin_per_type_num = 0
        if len(_to_merge_invnt_df) <= 1:
            return _to_merge_invnt_df, _last_bin_no, _to_merge_bin_per_type_num
        _to_merge_invnt_df, last_bin_no, _to_merge_bin_per_type_num = self.create_bins(
            _to_merge_invnt_df, last_bin_no+1, max_bin_qty)
        return _to_merge_invnt_df, last_bin_no, _to_merge_bin_per_type_num

    # @jit(nopython=True)
    def create_bins(self, invnt_df, bin_no, max_bin_qty):
        new_invnt_df = pd.DataFrame()
        sku_type = set(invnt_df.sku_id)
        _to_merge_bin_per_type_num = 0
        for sku_id in sku_type:
            # 该sku对应库存
            sku_invnt_df = invnt_df[invnt_df.sku_id.isin([sku_id])].copy()
            _to_merge_bin_per_type_num += len(set(sku_invnt_df['binNo']))
            # 初始化箱号
            sku_invnt_df.loc[:, 'binNo'] = -1
            sku_invnt_df = sku_invnt_df.sort_values(by='qty', ascending=False)
            sku_invnt_df.reset_index(inplace=True, drop=True)
            # print(sku_invnt_df)
            # 被理货的SKU所占箱数
            _to_merge_bin_per_type_num += len(set(sku_invnt_df['binNo']))
            sku_num = len(sku_invnt_df)
            qty = 0
            i = 0
            while i < sku_num:
                qty_tmp = qty + sku_invnt_df.at[i, 'qty']
                sku_invnt_df.at[i, 'binNo'] = bin_no
                if qty_tmp > max_bin_qty:
                    sku_invnt_df = sku_invnt_df.append(sku_invnt_df[i:i+1],
                                                       ignore_index=True)
                    sku_invnt_df.at[i, 'qty'] = max_bin_qty - qty
                    sku_invnt_df.at[sku_num, 'qty'] = qty_tmp - max_bin_qty
                    qty = 0
                    i += 1
                    bin_no += 1
                    sku_num += 1
                elif qty_tmp < max_bin_qty:
                    qty = qty_tmp
                    i += 1
                else:
                    qty = 0
                    i += 1
                    bin_no += 1
            new_invnt_df = new_invnt_df.append(sku_invnt_df, ignore_index=True)
        return new_invnt_df, bin_no, _to_merge_bin_per_type_num


class InventoryMatchSales():
    def __init__(self, daily_invnt, sales):
        self.invnt = pd.DataFrame()
        self.unsold = pd.DataFrame()
        self.deleted_invnt = pd.DataFrame()
        self.move_bin_num = 0
        self.sold_orders = 0
        self.sku_in_bin_num = 0
        self.deleted_qty = 0
        self.deleted_sku = 0
        self.deleted_order_line = 0
        self.avg_deleted_sku_in_bin = 0 #data.sku_in_bin_num / deleted_bin_num
        self.avg_deleted_qty_in_bin = 0 #deleted_qty / deleted_bin_num
        self.match_sales(daily_invnt, sales)

    def match_sales(self, daily_invnt_df, sales_df):
        invnt_df = daily_invnt_df.copy()
        sku_invnt = set(invnt_df['sku'])
        sales_df.set_index('sku', inplace=True)
        unsold_df = sales_df[~sales_df.index.isin(sku_invnt)].copy()
        sales_df = sales_df[sales_df.index.isin(sku_invnt)]
        # 取消sku索引
        sales_df.reset_index(inplace=True)
        unsold_df.reset_index(inplace=True)
        if sales_df.empty:
            self.invnt = invnt_df
            self.unsold = unsold_df
            return
        orders = set(sales_df['order'])
        deleted_invnt_df = pd.DataFrame()
        # unsold_index = []
        # deleted_invnt_index = []
        # 订单或库存全部清空，即存在list，否则用pd.append
        for order_ix in orders:
            # 找出该订单相应销售数据
            s_df = sales_df[sales_df['order'].isin([order_ix])]
            # print(s_df)
            s_sku_set = set(s_df['sku'])
            inv_df = invnt_df[invnt_df['sku'].isin(s_sku_set)]
            if inv_df.empty:
                # unsold_index = np.append(unsold_index, s_df.index.tolist())
                unsold_df = unsold_df.append(s_df, ignore_index=True)
                # print(unsold_index)
                continue
            # print(inv_df)
            # 库存非空，出库订单+1
            self.sold_orders += 1
            s_df.set_index('sku', inplace=True)
            # print(s_df)
            # 逻辑：先匹配库存少的箱子
            for sku_ix in s_sku_set:
                inv_sku_df = inv_df[inv_df['sku'].isin([sku_ix])]
                if inv_sku_df.empty:
                    # unsold_index = np.append(unsold_index, \
                    #     s_df.index.tolist())
                    unsold_df = unsold_df.append(s_df[s_df.index==sku_ix], ignore_index=True)
                    continue
                self.sku_in_bin_num += 1
                self.deleted_order_line += 1
                inv_sku_df = inv_sku_df.sort_values(by='qty')
                # 订单所需数量
                s_qty = s_df.at[sku_ix, 'qty']
                index_set = inv_sku_df.index.tolist()
                # print(index_set)
                # print(inv_sku_df)
                for inx in index_set:
                    inv_qty = inv_sku_df.at[inx, 'qty']
                    if s_qty > inv_qty:
                        s_qty -= inv_qty
                        # deleted_invnt_index = np.append(
                        #     deleted_invnt_index, inx)
                        deleted_invnt_df = deleted_invnt_df.append(invnt_df[invnt_df.index==inx], ignore_index=True)
                        invnt_df.drop(index=inx, inplace=True)
                        # invnt_df.at[inx, 'qty'] = 0
                        self.move_bin_num += 1
                    else:
                        # invnt_df.at[inx, 'qty'] = s_qty
                        # print(inx, invnt_df[invnt_df.index==inx])
                        tmp_df = invnt_df[invnt_df.index==inx].copy()
                        tmp_df.at[inx, 'qty'] = s_qty
                        deleted_invnt_df = deleted_invnt_df.append(tmp_df, ignore_index=True)
                        invnt_df.at[inx, 'qty'] = inv_qty - s_qty
                        # print(invnt_df[inx:inx+1])
                        # print(deleted_invnt_df)
                        s_qty = 0
                        self.move_bin_num += 1
                        break
                if s_qty > 0:
                    # print(sku_ix)
                    _tmp_df = s_df[s_df.index == sku_ix].copy()
                    _tmp_df.at[sku_ix, 'qty'] = s_qty
                    unsold_df = unsold_df.append(_tmp_df, ignore_index=True)
                invnt_df = invnt_df[invnt_df.qty > 0]

        # 更新未完成订单
        # unsold_index = set(unsold_index)
        # unsold_df = unsold_df.append(
        #     sales_df[sales_df.index.isin(unsold_index)], ignore_index=True)
        # # print(unsold_df)
        # # 更新库存
        # deleted_invnt_index = set(deleted_invnt_index)
        # deleted_invnt_df = deleted_invnt_df.append(
        #     daily_invnt_df[daily_invnt_df.index.isin(deleted_invnt_index)],
        #     ignore_index=True)
        # unsold_df.drop_duplicates(inplace=True)
        self.invnt = invnt_df
        self.unsold = unsold_df
        self.deleted_invnt = deleted_invnt_df
        self.deleted_qty = deleted_invnt_df.qty.sum()
        _tmpp = sales_df.qty.sum() - unsold_df.qty.sum()
        # print(_tmpp)
        # print(self.deleted_qty)
        self.deleted_sku = len(set(deleted_invnt_df['sku']))
        self.avg_deleted_sku_in_bin = self.sku_in_bin_num / self.move_bin_num
        self.avg_deleted_qty_in_bin = self.deleted_qty / self.move_bin_num
        # print(invnt_df.groupby(['sku_id', 'binNo'])['qty'].sum())


class CreateBin():
    def __init__(self, refund, last_bin_no, max_bin_qty):
        self.refund = pd.DataFrame()
        self.bin_num = -1
        self.last_bin_no = -1
        self.refund, self.bin_num, self.last_bin_no = \
            self.create_bins(refund, last_bin_no, max_bin_qty)

    # @jit(nopython=True)
    def create_bins(self, refund, last_bin_no, max_bin_qty):
        '''
        同年份同品类理为20件／箱
        大于20件的拆分，否则合并
        '''
        same_year_type_sku = set(refund['sku_year_type'])
        refund.loc[:, 'binNo'] = -1
        refund_to_inv = pd.DataFrame()
        bin_no = last_bin_no + 1
        for _sku in same_year_type_sku:
            same_sku_df = refund[refund['sku_year_type'].isin([_sku])].copy()
            same_sku_df = same_sku_df.sort_values(by='qty', ascending=False)
            same_sku_df.reset_index(inplace=True, drop=True)
            if same_sku_df.empty:
                continue
            if (len(same_sku_df) == 1) & (same_sku_df.at[0, 'qty'] <= max_bin_qty):
                same_sku_df.at[0, 'binNo'] = bin_no
                refund_to_inv = refund_to_inv.append(same_sku_df,
                                                 ignore_index=True)
                continue
            same_sku_num = len(same_sku_df)
            qty = 0
            i = 0
            while i < same_sku_num:
                qty_tmp = qty + same_sku_df.at[i, 'qty']
                same_sku_df.at[i, 'binNo'] = bin_no
                if qty_tmp > max_bin_qty:
                    same_sku_df = same_sku_df.append(same_sku_df[i:i+1],
                                                     ignore_index=True)
                    same_sku_df.at[i, 'qty'] = max_bin_qty - qty
                    same_sku_df.at[same_sku_num, 'qty'] = qty_tmp - max_bin_qty
                    qty = 0
                    i += 1
                    bin_no += 1
                    same_sku_num += 1

                elif qty_tmp < max_bin_qty:
                    qty = qty_tmp
                    i += 1
                else:
                    qty = 0
                    i += 1
                    bin_no += 1
            refund_to_inv = refund_to_inv.append(same_sku_df,
                                                 ignore_index=True)
        bin_num = bin_no - last_bin_no
        # print(refund_to_inv.groupby(['sku', 'binNo']).sum())
        return refund_to_inv, bin_num, bin_no

# @profile
def seasonal_refund_func(startDate, endDate):
    # 设置参数
    # 箱子容量
    max_bin_qty = 20
    # 最大箱号
    last_bin_no = 0
    # 每日库存
    daily_invnt_df = pd.DataFrame()
    # 每日输出数据
    daily_results_df = pd.DataFrame()
    # 监测库存件数
    monitor_sku_invnt = pd.DataFrame()

    # 建立数据库连接，准备存储结果
    db = pymysql.connect(host='0.0.0.0', user='root', passwd='root', db='JNBY')
    cursor = db.cursor()
    pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
    # engine = create_engine(
    #     'mysql+pymysql://root:root@172.20.8.216:3306/JY', encoding='utf8')
    engine = create_engine('mysql+pymysql://root:root@0.0.0.0:3306/JNBY',
                           encoding='utf8')

    #从数据库查询每日的订单和库存
    normal_orders = feather.read_feather(
        '/home/liqi/PythonProjects/JY/B2B_seasonal/normal_sale.feather')
    seasonal_refund = feather.read_feather(
        '/home/liqi/PythonProjects/JY/B2B_seasonal/season_refund_split.feather'
    )
    seasonal_refund.set_index('date', inplace=True)
    normal_orders.set_index('date', inplace=True)
    # print(seasonal_refund.head())

    for date in tqdm.tqdm(pd.date_range(start=startDate, end=endDate)):
        # 记录输出结果
        results = [date.strftime('%Y-%m-%d')]
        # 未完成订单
        unsold_df = pd.DataFrame()
        # 读取当天销售和退货数据
        refund = seasonal_refund[seasonal_refund.index == date].copy()
        sales_df = normal_orders[normal_orders.index == date].copy()
        # 删除数据库不必要信息
        if not refund.empty:
            seasonal_refund.drop(index=date, inplace=True)
        if not sales_df.empty:
            normal_orders.drop(index=date, inplace=True)
        refund.reset_index(inplace=True)
        sales_df.reset_index(inplace=True)
        # print(refund)
        # print(sales_df)
        #==================== 入库
        if not refund.empty:
            data = CreateBin(refund, last_bin_no, max_bin_qty)
            refund_df = data.refund
            # print(refund_df)
            last_bin_no = data.last_bin_no
            ''' 入库输出：
            入库件数：refund_qty
            入库sku数：refund_sku
            入库箱数：refund_bin
            '''
            refund_qty = refund_df.qty.sum()
            refund_sku = len(set(refund_df['sku']))
            refund_bin = data.bin_num
            results.extend([refund_qty, refund_sku, refund_bin])
            # 更新库存
            daily_invnt_df = daily_invnt_df.append(refund_df, ignore_index=True)
            # print(daily_invnt_df.groupby(['sku', 'binNo']).sum())
        else:
            results.extend([0, 0, 0])
        # print(results)

        #==================== 出库
        ''' 出库输出：
        出库件数：deleted_qty
        出库SKU数：deleted_sku
        出库行数：deleted_order_line
        出库订单数：deleted_order_num
        出库搬箱数：deleted_bin_num
        平均每出库箱命中SKU数：avg_deleted_sku_in_bin
        平均每出库箱命中件数：avg_deleted_qty_in_bin
        出库后剩余箱数：bins_after_sale
        '''
        if daily_invnt_df.empty:
            unsold_df.to_sql(name='unsold orders after seasonal',
                         con=engine,
                         if_exists='append',
                         chunksize=1000,
                         index=None)
            continue
        data = InventoryMatchSales(daily_invnt_df, sales_df)
        daily_invnt_df = data.invnt
        unsold_df = data.unsold
        # 输出数据
        deleted_bin_num = data.move_bin_num
        deleted_order_num = data.sold_orders
        deleted_qty = data.deleted_qty
        deleted_sku = data.deleted_sku
        deleted_order_line = data.deleted_order_line
        bins_after_sale = len(set(daily_invnt_df['binNo']))
        avg_deleted_sku_in_bin = round(data.avg_deleted_sku_in_bin, 2)
        avg_deleted_qty_in_bin = round(data.avg_deleted_qty_in_bin, 2)
        # deleted_invnt_df = data.deleted_invnt

        # 存储数据
        results.extend([
            deleted_qty, deleted_sku, deleted_order_line,
            deleted_order_num, deleted_bin_num, avg_deleted_sku_in_bin,
            avg_deleted_qty_in_bin, bins_after_sale
        ])
        # print(results)
        # print(daily_invnt_df.groupby(['sku', 'binNo']).sum())

        #==================== 理款
        ''' 理款输出：
        理款搬出箱数：to_merge_type_bin_num
        平均每款所占箱数：avg_type_bin_num
        平均每款件数：avg_type_qty_num
        理款后回库箱数：merged_type_bin_num
        '''
        data = MergeSku(daily_invnt_df, last_bin_no, max_bin_qty, 1)
        to_merge_type_bin_num = data.to_merge_type_bin_num
        avg_bin_per_type_num = round(data.avg_bin_per_type_num, 2)
        avg_qty_per_type_num = round(data.avg_qty_per_type_num, 2)
        merged_type_bin_num = data.merged_type_bin_num
        daily_invnt_df = data.invnt
        results.extend([
            to_merge_type_bin_num, avg_bin_per_type_num, avg_qty_per_type_num,
            merged_type_bin_num
        ])
        # print(results)
        # print(daily_invnt_df.groupby(['sku', 'binNo']).sum())

        #==================== 理SKU
        ''' 理SKU输出：
        被理货的SKU数量： to_merge_sku_num
        平均每个被理货的SKU所占的箱数：avg_bin_per_sku_num
        平均每个被理货的SKU的件数：avg_qty_per_sku_num
        理SKU搬出的箱数：to_merge_sku_bin_num
        理SKU后回库的箱数：merge_sku_bin_num
        '''

        # 逻辑：先删除监测列表中库存为0的sku，再将大于等于20件的sku加入监测列表中
        data = MergeSku(daily_invnt_df, last_bin_no, max_bin_qty, 2, monitor_sku_invnt)
        last_bin_no = data.last_bin_no
        daily_invnt_df = data.invnt
        # 输出所需数据
        to_merge_sku_num = data.to_merge_sku_num
        to_merge_sku_bin_num = data.to_merge_sku_bin_num
        merged_sku_bin_num = data.merged_sku_bin_num
        avg_bin_per_sku_num = round(data.avg_bin_per_sku_num, 2)
        avg_qty_per_sku_num = round(data.avg_qty_per_sku_num, 2)
        monitor_sku_invnt = data.monitor_sku_invnt

        results.extend([
            to_merge_sku_num, avg_bin_per_sku_num, avg_qty_per_sku_num,
            to_merge_sku_bin_num, merged_sku_bin_num
        ])
        # print(results)
        # print(unsold_df)
        daily_results_df = daily_results_df.append([results],
                                                   ignore_index=True)
        
        

        # 将未完成订单写入数据库
        # unsold_df.to_sql(name='unsold orders after seasonal',
        #                  con=engine,
        #                  if_exists='append',
        #                  chunksize=1000,
        #                  index=None)
    '''输出数据：
    入库件数：refund_qty
    入库sku数：refund_sku
    入库箱数：refund_bin
    出库件数：deleted_qty
    出库SKU数：deleted_sku
    出库行数：deleted_order_line
    出库订单数：deleted_order_num
    出库搬箱数：deleted_bin_num
    平均每出库箱命中SKU数：avg_deleted_sku_in_bin
    平均每出库箱命中件数：avg_deleted_qty_in_bin
    出库后剩余箱数：bins_after_sale
    理款搬出箱数：to_merge_type_bin_num
    平均每款所占箱数：avg_type_bin_num
    平均每款件数：avg_type_qty_num
    理款后回库箱数：merged_type_bin_num
    被理货的SKU数量： to_merge_sku_num
    平均每个被理货的SKU所占的箱数：avg_bin_per_sku_num
    平均每个被理货的SKU的件数：avg_qty_per_sku_num
    理SKU搬出的箱数：to_merge_sku_bin_num
    理SKU后回库的箱数：merge_sku_bin_num
    '''
    col = [
        'date', 'refund_qty', 'refund_sku', 'refund_bin', 'deleted_qty',
        'deleted_sku', 'deleted_order_line', 'deleted_order_num',
        'deleted_bin_num', 'avg_deleted_sku_in_bin', 'avg_deleted_qty_in_bin',
        'bins_after_sale', 'to_merge_type_bin_num', 'avg_type_bin_num',
        'avg_type_qty_num', 'merged_type_bin_num', 'to_merge_sku_num',
        'avg_bin_per_sku_num', 'avg_qty_per_sku_num', 'to_merge_sku_bin_num',
        'merge_sku_bin_num'
    ]
    daily_results_df.columns = col
    # print(daily_results_df)
    # print(daily_invnt_df)
    daily_results_df.to_sql(name='daily results for B2B seasonal',
                            con=engine,
                            if_exists='append',
                            chunksize=1000,
                            index=None)
    # 将每日库存明细写入数据库
    daily_invnt_df.to_sql(name='invnt_after_seasonal_3_5',
                            con=engine,
                            if_exists='append',
                            chunksize=1000,
                            index=None)

def main():
    startDate = '2020-03-01'
    endDate = '2020-05-31'
    seasonal_refund_func(startDate, endDate)

if __name__ == "__main__":
    main()
