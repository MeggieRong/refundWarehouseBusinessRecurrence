import pandas as pd

from return_order import *
from sale_order import *
from inventory import *
from statistic import *
from seasons import *


def write_to_csv(filename, fields, data):
    with open(filename, 'w', newline='') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerows(data)


FALL_WINTER_CAPACITY = 8
SPRING_SUMMER_CAPACITY = 15

sku_season_querier = SkuSeasonQuerier()

return_order_manager = ReturnOrderManager(sku_season_querier)
return_filename = 'input/b2c_returns.csv'
return_order_manager.read_return_order_file(return_filename)

vip_return_order_manager = ReturnOrderManager(sku_season_querier)
vip_return_order_filename = 'input/唯品退货数据1-12.csv'
vip_return_order_manager.read_vip_return_order_file(vip_return_order_filename)

b2b_return_order_manager = ReturnOrderManager(sku_season_querier)
b2b_return_filename = 'input/b2b_returns_processed.csv'
b2b_return_order_manager.read_b2b_return_order_file(b2b_return_filename)

sale_order_manager = SaleOrderManager()
sale_order_filename = 'input/b2c_outbounds_processed.csv'
sale_order_manager.read_sale_order_file(sale_order_filename)

daily_inbound_fields = ['date', 'inbound_item_num', 'inbound_sku_num', 'inbound_bin_num']
b2c_daily_inbound_data = []  # 日期，入库件数，入库SKU数，入库箱子数
vip_daily_inbound_data = []  # 日期，入库件数，入库SKU数，入库箱子数

daily_outbound_fields = ['date', 'mean_bin_hitting_sku_num', 'mean_bin_hitting_item_num', 'outbound_bin_num',
                         'outbound_item_num']
daily_outbound_data = []  # 日期，平均每箱命中SKU数，平均每箱命中件数，出库箱数，出库件数

not_matching_fields = ['date', 'order_no', 'SKU', 'amount']
not_matching_data = []  # 日期，订单编号，SKU，件数

merge_bin_inv_fields = ['date', 'merging_bin_num', 'SKU_num', 'bin_num', 'stock_item_num']
merge_bin_inv_data = []  # 日期，合并箱子数，SKU数，箱数，库存件数

b2b_daily_inbound_fields = ['date', 'inbound_item_num', 'inbound_sku_num', 'inbound_bin_num']
b2b_daily_inbound_data = []  # 日期，入库件数，入库SKU数，入库箱子数

b2b_left_inv_fields = ['date', 'SKU_num', 'bin_num', 'stock_item_num']
b2b_left_inv_data = []  # 日期，SKU数，箱数，库存件数

b2b_daily_outbound_fields = ['date', 'mean_bin_hitting_sku_num', 'mean_bin_hitting_item_num', 'outbound_bin_num',
                             'outbound_item_num']
b2b_daily_outbound_data = []  # 日期，平均每箱命中SKU数，平均每箱命中件数，出库箱数，出库件数

b2b_clean_inv_detail_fields = ['date', 'bin_id', 'sku', 'amount']
b2b_clean_inv_detail_data = []  # 日期，箱子ID，SKU，件数

b2b_daily_sort_bin_fields = ['date', 'daily_sorted_sku_num', 'daily_sorted_item_num', 'daily_move_out_bin_num',
                             'daily_move_in_bin_num']
b2b_daily_sort_bin_data = []  # 日期，理库SKU数，理库件数，理库碰动箱数，理库后回库箱数

b2b_unshelf_fields = ['date', 'have_full_bin_sku_num', 'full_bin_num', 'full_bin_item_num',
                      'style_num(not include full bins)', 'item_num(not include full bins)',
                      'mean_item_num_per_bin_after_sort(not include full bins)']
b2b_unshelf_data = []

start_date = '2020/1/1'
end_date = '2020/12/31'
pd.date_range(start=start_date, end=end_date)

clean_b2b_inv_date_set = {'31/5/2020', '31/8/2020', '30/11/2020'}

inv_manager = InvManager(FALL_WINTER_CAPACITY, SPRING_SUMMER_CAPACITY, sku_season_querier)
sale_order_matcher = SaleOrderMatcher(inv_manager)

b2b_inv_manager = InvManager(FALL_WINTER_CAPACITY, SPRING_SUMMER_CAPACITY, sku_season_querier)
b2b_sale_order_matcher = SaleOrderMatcher(b2b_inv_manager)
for date in pd.date_range(start=start_date, end=end_date):
    date_str = str(date.day) + "/" + str(date.month) + "/" + str(date.year)
    # 获取当天退货订单数据
    return_orderlines = return_order_manager.get_one_day_order_lines(date_str)
    fall_winter_bin_stock_generator = BinStockGenerator(FALL_WINTER_CAPACITY)
    spring_summer_bin_stock_generator = BinStockGenerator(SPRING_SUMMER_CAPACITY)
    for orderline in return_orderlines:
        if orderline.season.__contains__('秋') or orderline.season.__contains__('冬'):
            fall_winter_bin_stock_generator.add_inv(orderline.sku, orderline.amount)
        elif orderline.season.__contains__('春') or orderline.season.__contains__('夏'):
            spring_summer_bin_stock_generator.add_inv(orderline.sku, orderline.amount)
    # 记录B2C入库数据
    b2c_daily_inbound_data.append(
        [date_str, fall_winter_bin_stock_generator.total_amount + spring_summer_bin_stock_generator.total_amount,
         len(fall_winter_bin_stock_generator.sku_amount) + len(spring_summer_bin_stock_generator.sku_amount),
         len(fall_winter_bin_stock_generator.bins) + len(spring_summer_bin_stock_generator.bins)])
    print("Date: ", date_str, " 入库件数：", b2c_daily_inbound_data[-1][1],
          " 入库SKU数：", b2c_daily_inbound_data[-1][2],
          " 入库箱子数：", b2c_daily_inbound_data[-1][3]
          )
    # 更新库存
    inv_manager.add_inv(fall_winter_bin_stock_generator.bins)
    inv_manager.add_inv(spring_summer_bin_stock_generator.bins)
    # 获取当天唯品会退货订单数据
    vip_return_orderlines = vip_return_order_manager.get_one_day_order_lines(date_str)
    vip_fall_winter_bin_stock_generator = BinStockGenerator(FALL_WINTER_CAPACITY)
    vip_spring_summer_bin_stock_generator = BinStockGenerator(SPRING_SUMMER_CAPACITY)
    for orderline in vip_return_orderlines:
        if orderline.season.__contains__('秋') or orderline.season.__contains__('冬'):
            vip_fall_winter_bin_stock_generator.add_inv(orderline.sku, orderline.amount)
        elif orderline.season.__contains__('春') or orderline.season.__contains__('夏'):
            vip_spring_summer_bin_stock_generator.add_inv(orderline.sku, orderline.amount)
    # 记录唯品会入库数据
    vip_daily_inbound_data.append(
        [date_str,
         vip_fall_winter_bin_stock_generator.total_amount + vip_spring_summer_bin_stock_generator.total_amount,
         len(vip_fall_winter_bin_stock_generator.sku_amount) + len(vip_spring_summer_bin_stock_generator.sku_amount),
         len(vip_fall_winter_bin_stock_generator.bins) + len(vip_spring_summer_bin_stock_generator.bins)])
    print("唯品会，Date: ", date_str, " 入库件数：", vip_daily_inbound_data[-1][1],
          " 入库SKU数：", vip_daily_inbound_data[-1][2],
          " 入库箱子数：", vip_daily_inbound_data[-1][3]
          )
    # 更新库存
    inv_manager.add_inv(vip_fall_winter_bin_stock_generator.bins)
    inv_manager.add_inv(vip_spring_summer_bin_stock_generator.bins)
    # 获取当天出库订单数据
    sale_order_lines = sale_order_manager.get_one_day_orderlines(date_str)
    # 获得匹配的订单行
    total_matching_lines, total_not_matching_lines = sale_order_matcher.match_sale_orderlines(sale_order_lines,
                                                                                              date_str)
    # 数据统计
    bin_mean_sku_num, bin_mean_item_num, total_bin_num, total_item_num = \
        statistic_outbound_data(total_matching_lines)
    daily_outbound_data.append([date_str, bin_mean_sku_num, bin_mean_item_num, total_bin_num, total_item_num])
    for line in total_not_matching_lines:
        not_matching_data.append([date_str, line.order_no, line.sku, line.amount])
    # 合并箱子
    fall_winter_merge_result = inv_manager.merge_bins("秋冬", FALL_WINTER_CAPACITY)
    spring_summer_merge_result = inv_manager.merge_bins("春夏", SPRING_SUMMER_CAPACITY)
    merge_result = fall_winter_merge_result + spring_summer_merge_result
    merge_bin_num = 0
    for entry in merge_result:
        merge_bin_num += len(entry.bins)
    merge_bin_inv_data.append([date_str, merge_bin_num, inv_manager.get_sku_num(),
                               inv_manager.get_bin_num(), inv_manager.get_total_item_num()])

    # 获取当天B2B退货订单数据
    b2b_return_orderlines = b2b_return_order_manager.get_one_day_order_lines(date_str)
    b2b_fall_winter_bin_stock_generator = BinStockGenerator(FALL_WINTER_CAPACITY)
    b2b_spring_summer_bin_stock_generator = BinStockGenerator(SPRING_SUMMER_CAPACITY)
    for orderline in b2b_return_orderlines:
        if orderline.season.__contains__('秋') or orderline.season.__contains__('冬'):
            b2b_fall_winter_bin_stock_generator.add_inv(orderline.sku, orderline.amount)
        elif orderline.season.__contains__('春') or orderline.season.__contains__('夏'):
            b2b_spring_summer_bin_stock_generator.add_inv(orderline.sku, orderline.amount)

    # 记录B2B入库数据
    b2b_daily_inbound_data.append([date_str,
                                   b2b_fall_winter_bin_stock_generator.total_amount + b2b_spring_summer_bin_stock_generator.total_amount,
                                   len(b2b_fall_winter_bin_stock_generator.sku_amount) + len(
                                       b2b_spring_summer_bin_stock_generator.sku_amount),
                                   len(b2b_fall_winter_bin_stock_generator.bins) + len(
                                       b2b_spring_summer_bin_stock_generator.bins)])
    print("[B2B] Date: ", date_str, " 入库件数：", b2b_daily_inbound_data[-1][0],
          " 入库SKU数：", b2b_daily_inbound_data[-1][1],
          " 入库箱子数：", b2b_daily_inbound_data[-1][2]
          )
    # 更新B2B库存
    b2b_inv_manager.add_inv(b2b_fall_winter_bin_stock_generator.bins)
    b2b_inv_manager.add_inv(b2b_spring_summer_bin_stock_generator.bins)
    # 获得匹配的B2B订单行
    total_b2b_matching_lines, total_b2b_not_matching_lines = b2b_sale_order_matcher.match_sale_orderlines(
        total_not_matching_lines,
        date_str)
    # 数据统计
    bin_mean_sku_num, bin_mean_item_num, total_bin_num, total_item_num = \
        statistic_outbound_data(total_b2b_matching_lines)
    b2b_daily_outbound_data.append([date_str, bin_mean_sku_num, bin_mean_item_num, total_bin_num, total_item_num])
    # B2B每日理库
    sorted_sku_num, sorted_item_num, move_out_bin_num, move_in_bin_num = b2b_inv_manager.b2b_daily_sort()
    b2b_daily_sort_bin_data.append([date_str, sorted_sku_num, sorted_item_num, move_out_bin_num, move_in_bin_num])

    # 保存结余库存
    b2b_left_inv_data.append([date_str, b2b_inv_manager.get_sku_num(),
                              b2b_inv_manager.get_bin_num(), b2b_inv_manager.get_total_item_num()])
    if clean_b2b_inv_date_set.__contains__(date_str):
        result = b2b_inv_manager.b2b_sort_before_un_shelve()
        before_unshelf_data_fields = ['style_code', 'bin_num', 'total_item_num', 'mean_item_num_per_bin']
        write_to_csv('result/B2B下架理库前数据' + date_str.replace('/', '_') + '.csv', before_unshelf_data_fields, result[0])
        b2b_unshelf_data.append([date_str, result[1], result[2], result[3], result[4], result[5], result[6]])
        # # 保存下架当天库存明细
        # for sku, bin_amount in b2b_inv_manager.sku_bin_amount.items():
        #     for bin, amount in bin_amount.items():
        #         b2b_clean_inv_detail_data.append([date_str, bin, sku, amount])
        # 清空B2B库存
        b2b_inv_manager.sku_bin_amount = {}
        b2b_inv_manager.bin_to_sku_set = {}
        b2b_inv_manager.bin_item_num = {}

write_to_csv('result/B2C入库数据.csv', daily_inbound_fields, b2c_daily_inbound_data)
write_to_csv('result/唯品会入库数据.csv', daily_inbound_fields, vip_daily_inbound_data)
write_to_csv('result/B2C出库数据.csv', daily_outbound_fields, daily_outbound_data)
write_to_csv('result/B2C未完成订单数据.csv', not_matching_fields, not_matching_data)
write_to_csv('result/B2C结余库存处理.csv', merge_bin_inv_fields, merge_bin_inv_data)
write_to_csv('result/B2B入库数据.csv', b2b_daily_inbound_fields, b2b_daily_inbound_data)
write_to_csv('result/B2B每日结余库存.csv', b2b_left_inv_fields, b2b_left_inv_data)
write_to_csv('result/B2B出库数据.csv', b2b_daily_outbound_fields, b2b_daily_outbound_data)
write_to_csv('result/B2B每日理库数据.csv', b2b_daily_sort_bin_fields, b2b_daily_sort_bin_data)
# write_to_csv('result/B2B下架当天库存明细.csv', b2b_clean_inv_detail_fields, b2b_clean_inv_detail_data)
write_to_csv('result/B2B下架数据.csv', b2b_unshelf_fields, b2b_unshelf_data)
