import csv
from seasons import *


class ReturnOrderLine:
    def __init__(self, line_str_list):
        self.date = line_str_list[1]
        self.order_no = line_str_list[2]
        self.sku = line_str_list[3]
        self.season = line_str_list[6]
        self.amount = int(line_str_list[8])
        # self.date = line[0]
        # self.order_no = line[1]
        # self.sku = line[2]
        # self.amount = int(line[7])


class VIPReturnOrderLine:
    def __init__(self, line_str_list):
        date_list = line_str_list[0].split('/')
        self.date = str(date_list[2]) + "/" + str(date_list[1]) + "/" + str(date_list[0])
        self.order_no = line_str_list[1]
        self.sku = line_str_list[2]
        self.season = line_str_list[3]
        self.amount = int(line_str_list[7])


class B2BReturnOrderLine:
    def __init__(self, line_str_list):
        self.date = line_str_list[1]
        self.order_no = line_str_list[2]
        self.sku = line_str_list[4]
        self.season = line_str_list[7]
        self.amount = int(line_str_list[9])
        # self.date = line[0]
        # self.order_no = line[1]
        # self.sku = line[3]
        # self.amount = int(line[8])


class ReturnOrderManager:
    def __init__(self, sku_season_querier: SkuSeasonQuerier):
        self.return_order_lines = []
        self.date_to_order_lines = {}
        self.sku_season_querier = sku_season_querier

    def read_return_order_file(self, filename):
        return_order_file = open(filename, "r", encoding='UTF-8')
        return_order_reader = csv.reader(return_order_file)
        first_line = True
        for line_str_list in return_order_reader:
            if first_line:
                first_line = False
                continue
            order_line = ReturnOrderLine(line_str_list)
            self.sku_season_querier.add_sku_season(order_line.sku, order_line.season)
            self.return_order_lines.append(order_line)
            if not self.date_to_order_lines.__contains__(order_line.date):
                self.date_to_order_lines[order_line.date] = [order_line]
            else:
                self.date_to_order_lines[order_line.date].append(order_line)
        return_order_file.close()

    def read_vip_return_order_file(self, filename):
        return_order_file = open(filename, "r", encoding='UTF-8')
        return_order_reader = csv.reader(return_order_file)
        first_line = True
        for line_str_list in return_order_reader:
            if first_line:
                first_line = False
                continue
            order_line = VIPReturnOrderLine(line_str_list)
            self.sku_season_querier.add_sku_season(order_line.sku, order_line.season)
            self.return_order_lines.append(order_line)
            if not self.date_to_order_lines.__contains__(order_line.date):
                self.date_to_order_lines[order_line.date] = [order_line]
            else:
                self.date_to_order_lines[order_line.date].append(order_line)
        return_order_file.close()

    def read_b2b_return_order_file(self, filename):
        return_order_file = open(filename, "r", encoding='UTF-8')
        return_order_reader = csv.reader(return_order_file)
        first_line = True
        for line_str_list in return_order_reader:
            if first_line:
                first_line = False
                continue
            order_line = B2BReturnOrderLine(line_str_list)
            self.sku_season_querier.add_sku_season(order_line.sku, order_line.season)
            self.return_order_lines.append(order_line)
            if not self.date_to_order_lines.__contains__(order_line.date):
                self.date_to_order_lines[order_line.date] = [order_line]
            else:
                self.date_to_order_lines[order_line.date].append(order_line)
        return_order_file.close()

    def get_one_day_order_lines(self, date):
        if self.date_to_order_lines.__contains__(date):
            return self.date_to_order_lines[date]
        return []
