import csv
from inventory import *

class SaleOrderMatchingLine:
    def __init__(self, order_no, sku, bin_id, amount):
        self.order_no = order_no
        self.sku = sku
        self.bin_id = bin_id
        self.amount = amount


class SaleOrderNotMatchingline:
    def __init__(self, date, order_no, sku, amount):
        self.date = date
        self.order_no = order_no
        self.sku = sku
        self.amount = amount


class SaleOrderLine:
    def __init__(self, line):
        self.date = line[1]
        self.order_no = line[2]
        self.sku = line[4]
        self.amount = int(line[9])
        # self.date = line[0]
        # self.order_no = line[1]
        # self.sku = line[3]
        # self.amount = int(line[8])


class SaleOrderManager:
    def __init__(self):
        self.sale_orderlines = []
        self.date_to_orderlines = {}

    def read_sale_order_file(self, filename):
        sale_order_file = open(filename, "r", encoding='UTF-8')
        sale_order_reader = csv.reader(sale_order_file)
        first_line = True
        for line in sale_order_reader:
            if first_line:
                first_line = False
                continue
            orderline = SaleOrderLine(line)
            self.sale_orderlines.append(orderline)
            if not self.date_to_orderlines.__contains__(orderline.date):
                self.date_to_orderlines[orderline.date] = [orderline]
            else:
                self.date_to_orderlines[orderline.date].append(orderline)
        sale_order_file.close()

    def get_one_day_orderlines(self, date):
        if not self.date_to_orderlines.__contains__(date):
            return []
        return self.date_to_orderlines[date]


class SaleOrderMatcher:
    def __init__(self, inv_manager: InvManager):
        self.inv_manager = inv_manager

    def match_sale_orderlines(self, sale_orderlines, date_str):
        total_matching_lines = []
        total_not_matching_lines = []
        for orderline in sale_orderlines:
            matching_lines, not_matching_orderline = self.match_sku_amount(date_str, orderline.order_no,
                                                                           orderline.sku, orderline.amount)
            if not_matching_orderline.amount > 0:
                total_not_matching_lines.append(not_matching_orderline)
            self.inv_manager.subtract_stock(matching_lines)
            total_matching_lines.extend(matching_lines)
        return total_matching_lines, total_not_matching_lines

    def match_sku_amount(self, date_str, order_no, sku, amount):
        left_amount = amount
        result = []
        bin_amount = self.inv_manager.get_sku_stock(sku)
        if len(bin_amount) == 0:
            return result, SaleOrderNotMatchingline(date_str, order_no, sku, amount)
        for bin_id, amount in bin_amount.items():
            matching_amount = min(left_amount, amount)
            matching_line = SaleOrderMatchingLine(order_no, sku, bin_id, matching_amount)
            result.append(matching_line)
            left_amount -= matching_amount
            assert (left_amount >= 0)
            if left_amount == 0:
                break
        not_matching_orderline = SaleOrderNotMatchingline(date_str, order_no, sku, left_amount)
        return result, not_matching_orderline

