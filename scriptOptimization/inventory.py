import math
from seasons import *


def get_style_code(sku):
    return sku[:9]


def get_color_code(sku):
    return sku[9:12]


def get_size_code(sku):
    return sku[12:]


class BinStock:
    next_bin_id = 1

    def __init__(self):
        self.bin_id = self.get_next_bin_id()
        self.sku_amount = {}
        self.total_amount = 0

    def get_total_amount(self):
        return self.total_amount

    def add_inv(self, sku, amount):
        if self.sku_amount.__contains__(sku):
            self.sku_amount[sku] += amount
        else:
            self.sku_amount[sku] = amount
        self.total_amount += amount

    def get_color_set(self):
        color_set = set()
        for sku, amount in self.sku_amount.items():
            color = get_color_code(sku)
            color_set.add(color)
        return color_set

    @classmethod
    def get_next_bin_id(cls):
        result = cls.next_bin_id
        cls.next_bin_id += 1
        return result


class BinStockGenerator:
    def __init__(self, bin_max_item_num):
        self.bins = []
        self.bin_max_item_num = bin_max_item_num
        self.sku_amount = {}
        self.total_amount = 0

    def add_inv(self, sku, amount):
        if amount == 0:
            return

        if not self.sku_amount.__contains__(sku):
            self.sku_amount[sku] = 0
        self.sku_amount[sku] += amount

        if len(self.bins) == 0:
            self.bins = [BinStock()]
        if self.bins[-1].get_total_amount() == self.bin_max_item_num:
            self.bins.append(BinStock())
        left_amount = self.bin_max_item_num - self.bins[-1].get_total_amount()
        assert (left_amount > 0)
        if amount <= left_amount:
            self.bins[-1].add_inv(sku, amount)
            self.total_amount += amount
        else:
            self.bins[-1].add_inv(sku, left_amount)
            self.total_amount += left_amount
            self.bins.append(BinStock())
            self.add_inv(sku, amount - left_amount)


class MergedBins:
    def __init__(self):
        self.bins = []
        self.total_amount = 0

    def add_bin(self, bin_id, amount):
        self.bins.append(bin_id)
        self.total_amount += amount


class InvManager:
    def __init__(self, fall_winter_capacity, spring_summer_capacity, sku_season_querier: SkuSeasonQuerier):
        self.sku_season_querier = sku_season_querier
        self.sku_bin_amount = {}
        self.bin_item_num = {}
        self.bin_to_sku_set = {}
        self.bin_sorted = {}  # bin id -> bool: is this bin sorted
        self.fall_winter_capacity = fall_winter_capacity
        self.spring_summer_capacity = spring_summer_capacity

    # 往某箱子中添加某SKU若干件
    def add_bin_sku_amount(self, bin_id, sku, amount):
        if not self.sku_bin_amount.__contains__(sku):
            self.sku_bin_amount[sku] = {}
        if not self.sku_bin_amount[sku].__contains__(bin_id):
            self.sku_bin_amount[sku][bin_id] = 0
        self.sku_bin_amount[sku][bin_id] += amount
        if not self.bin_item_num.__contains__(bin_id):
            self.bin_item_num[bin_id] = 0
        self.bin_item_num[bin_id] += amount
        if not self.bin_to_sku_set.__contains__(bin_id):
            self.bin_to_sku_set[bin_id] = set()
        self.bin_to_sku_set[bin_id].add(sku)

    # 从某箱子移除某SKU若干件
    def remove_bin_sku_amount(self, bin_id, sku, amount):
        sort_threshold = int(self.get_sku_bin_capacity(sku) / 2)
        self.sku_bin_amount[sku][bin_id] -= amount
        self.bin_item_num[bin_id] -= amount
        if self.sku_bin_amount[sku][bin_id] == 0:
            del self.sku_bin_amount[sku][bin_id]
            self.bin_to_sku_set[bin_id].remove(sku)
        if len(self.sku_bin_amount[sku]) == 0:
            del self.sku_bin_amount[sku]
        if self.bin_item_num[bin_id] == 0:
            del self.bin_item_num[bin_id]
            self.bin_sorted.pop(bin_id, None)
        elif self.is_bin_sorted(bin_id) and self.bin_item_num[bin_id] <= sort_threshold:
            self.set_bin_not_sorted(bin_id)
        if len(self.bin_to_sku_set[bin_id]) == 0:
            del self.bin_to_sku_set[bin_id]

    def is_bin_sorted(self, bin_id):
        if self.bin_sorted.__contains__(bin_id):
            return self.bin_sorted[bin_id]
        return False

    def set_bin_sorted(self, bin_id):
        self.bin_sorted[bin_id] = True

    def set_bin_not_sorted(self, bin_id):
        self.bin_sorted[bin_id] = False

    def add_inv(self, bins):
        for bin_stock in bins:
            for sku, amount in bin_stock.sku_amount.items():
                self.add_bin_sku_amount(bin_stock.bin_id, sku, amount)

    def get_sku_stock(self, sku):
        if not self.sku_bin_amount.__contains__(sku):
            return {}
        return self.sku_bin_amount[sku]

    def subtract_stock(self, sale_matching_lines):
        for line in sale_matching_lines:
            self.remove_bin_sku_amount(line.bin_id, line.sku, line.amount)

    def get_sku_num(self):
        return len(self.sku_bin_amount)

    def get_bin_num(self):
        return len(self.bin_item_num)

    def get_total_item_num(self):
        total_item_num = 0
        for bin_id, item_num in self.bin_item_num.items():
            total_item_num += item_num
        return total_item_num

    def get_bin_season(self, bin_id):
        sku = next(iter(self.bin_to_sku_set[bin_id]))  # 从箱子中挑选一个SKU
        return self.sku_season_querier.get_sku_season(sku)

    def merge_bins(self, seasons, bin_capacity):
        merge_result = [MergedBins()]
        bin_id_item_num_list = []
        for bin, item_num in self.bin_item_num.items():
            bin_season = self.get_bin_season(bin)
            if bin_season.__contains__(seasons[0]) or bin_season.__contains__(seasons[1]):
                bin_id_item_num_list.append([bin, item_num])
        bin_id_item_num_list.sort(key=lambda x: x[1])  # 根据箱子内件数从小到大排序
        half_capacity = math.floor(bin_capacity / 2)

        for bin_id, item_num in bin_id_item_num_list:
            if item_num > half_capacity:
                break
            if merge_result[-1].total_amount + item_num <= bin_capacity:
                merge_result[-1].add_bin(bin_id, item_num)
            elif len(merge_result[-1].bins) <= 1:
                merge_result.pop()
                break
            else:
                merge_result.append(MergedBins())
                merge_result[-1].add_bin(bin_id, item_num)

        if len(merge_result) > 0 and len(merge_result[-1].bins) <= 1:
            merge_result.pop()
        # 更新库存结构
        for entry in merge_result:
            self.update_inv_from_merge_bins(entry)
        return merge_result

    def is_bin_exist(self, bin_id):
        return self.bin_item_num.__contains__(bin_id)

    def update_inv_from_merge_bins(self, merged_bins: MergedBins):
        if len(merged_bins.bins) <= 1:
            return
        is_first = True
        first_bin_id = merged_bins.bins[0]
        for bin_id in merged_bins.bins:
            if is_first:
                is_first = False
                continue
            skus = self.bin_to_sku_set[bin_id].copy()
            for sku in skus:
                # 将这个箱子的SKU挪到第一个箱子里面
                amount = self.sku_bin_amount[sku][bin_id]
                self.remove_bin_sku_amount(bin_id, sku, amount)
                self.add_bin_sku_amount(first_bin_id, sku, amount)

    def get_sku_not_sorted_stock(self, sku):
        if not self.sku_bin_amount.__contains__(sku):
            return 0
        not_sorted_amount = 0
        for bin_id, amount in self.sku_bin_amount[sku].items():
            if not self.is_bin_sorted(bin_id):
                not_sorted_amount += amount
        return not_sorted_amount

    def b2b_sort_sku(self, sku, not_sorted_amount):
        bin_capacity = self.get_sku_bin_capacity(sku)
        # 取出装有此SKU的且未理过的箱子
        bin_amount_list = list(self.sku_bin_amount[sku].items())
        # 按照箱子内SKU的件数从小到大排序
        bin_amount_list.sort(key=lambda x: x[1])
        left_amount = not_sorted_amount % bin_capacity
        # 计算可以排除的箱子
        excluded_bins = set()
        for bin_id, amount in bin_amount_list:
            if self.is_bin_sorted(bin_id):
                excluded_bins.add(bin_id)
                continue
            if amount <= left_amount:
                excluded_bins.add(bin_id)
                left_amount -= amount
            else:
                break
        # 对于剩下的箱子尝试将SKU理到新的箱子里面
        new_bin_num = int(not_sorted_amount / bin_capacity)  # 新箱子的数量
        sorted_item_num = new_bin_num * bin_capacity  # 理论上理库的数量
        left_num = sorted_item_num
        move_out_bin_num = 0  # 搬出箱子的数量
        move_in_bin_num = new_bin_num  # 搬入箱子的数量：原箱数量（排除空箱）+ 新箱数量
        for bin_id, amount in bin_amount_list:
            if excluded_bins.__contains__(bin_id):
                continue
            move_out_bin_num += 1
            if left_num > 0:
                sort_item_num = min(left_num, amount)
                self.remove_bin_sku_amount(bin_id, sku, sort_item_num)  # 扣减原箱库存
                left_num -= sort_item_num
                if self.is_bin_exist(bin_id):  # 判断是否为空箱
                    move_out_bin_num += 1
            else:
                break
        assert (left_num == 0)
        # 构建新箱库存
        new_sorted_bins = [BinStock() for i in range(new_bin_num)]
        # 将新箱库存加入总库存
        for i in range(new_bin_num):
            new_sorted_bins[i].add_inv(sku, bin_capacity)
        self.add_inv(new_sorted_bins)
        # 将这些新的箱子标记为已理库
        for bin in new_sorted_bins:
            self.set_bin_sorted(bin.bin_id)
        return sorted_item_num, move_out_bin_num, move_in_bin_num

    # 每日监控SKU库存深度，当某SKU未理过的库存达到15件，则将该SKU理为一箱回库（放到新的料箱中），
    # 理库碰动料箱若空箱则取出，否则原箱回库
    def b2b_daily_sort(self):
        sorted_sku_num = 0  # 每日理库的SKU数
        total_sorted_item_num = 0
        total_move_out_bin_num = 0
        total_move_in_bin_num = 0
        skus = list(self.sku_bin_amount.keys())
        for sku in skus:
            season = self.sku_season_querier.get_sku_season(sku)
            if season.__contains__('秋') or season.__contains__('冬'):
                bin_capacity = self.fall_winter_capacity
            else:
                bin_capacity = self.spring_summer_capacity
            not_sorted_amount = self.get_sku_not_sorted_stock(sku)
            if not_sorted_amount >= bin_capacity:
                sorted_item_num, move_out_bin_num, move_in_bin_num = self.b2b_sort_sku(sku, not_sorted_amount)
                total_sorted_item_num += sorted_item_num
                total_move_out_bin_num += move_out_bin_num
                total_move_in_bin_num += move_in_bin_num
                sorted_sku_num += 1
        return sorted_sku_num, total_sorted_item_num, total_move_out_bin_num, total_move_in_bin_num

    def get_sku_bin_capacity(self, sku):
        season = self.sku_season_querier.get_sku_season(sku)
        if season.__contains__('秋') or season.__contains__('冬'):
            return self.fall_winter_capacity
        else:
            return self.spring_summer_capacity

    def get_sku_total_item_num_exclude_full_bins(self, sku):
        bin_capacity = self.get_sku_bin_capacity(sku)
        if not self.sku_bin_amount.__contains__(sku):
            return 0
        total_item_num_exclude_full_bins = 0
        total_bin_num_exclude_full_bins = 0
        full_bin_num = 0
        for bin, amount in self.sku_bin_amount[sku].items():
            assert (amount <= bin_capacity)
            if amount < bin_capacity:
                total_item_num_exclude_full_bins += amount
                total_bin_num_exclude_full_bins += 1
            else:
                full_bin_num += 1
        return total_item_num_exclude_full_bins, total_bin_num_exclude_full_bins, full_bin_num

    def b2b_sort(self, sku_amount, bin_capacity):
        full_bins = []
        unfull_bins = [BinStock()]
        for sku, amount in sku_amount:
            left_amount = amount
            sku_color = get_color_code(sku)
            # 针对此SKU，遍历现有未满的箱子
            full_bin_index = []
            for i in range(len(unfull_bins)):
                bin_left_amount = bin_capacity - unfull_bins[i].get_total_amount()
                assert (bin_left_amount > 0)
                if unfull_bins[i].get_color_set().__contains__(sku_color):  # 若颜色相同则不能放
                    continue
                if bin_left_amount > left_amount:
                    unfull_bins[i].add_inv(sku, left_amount)
                    left_amount = 0
                    break
                elif bin_left_amount == left_amount:
                    unfull_bins[i].add_inv(sku, bin_left_amount)
                    full_bin_index.append(i)
                    left_amount = 0
                    break
                else:
                    unfull_bins[i].add_inv(sku, bin_left_amount)
                    full_bin_index.append(i)
                    left_amount -= bin_left_amount
            # 将满箱移除
            for i in full_bin_index:
                full_bins.append(unfull_bins[i])
            for i in sorted(full_bin_index, reverse=True):
                del unfull_bins[i]
            while left_amount > 0:
                fill_num = min(left_amount, bin_capacity)
                if fill_num == bin_capacity:
                    full_bins.append(BinStock())
                    full_bins[-1].add_inv(sku, bin_capacity)
                else:
                    unfull_bins.append(BinStock())
                    unfull_bins[-1].add_inv(sku, fill_num)
                left_amount -= fill_num
        full_bins.extend(unfull_bins)
        return full_bins

    # B2B 针对某一款进行理库
    def b2b_sort_for_the_style(self, skus, bin_capacity):
        sku = next(iter(skus))  # 挑选一个SKU
        bin_capacity = self.get_sku_bin_capacity(sku)
        sku_amount = []
        total_item_num = 0  # 此变量用于存放此款的总件数（排除单SKU整箱）
        total_bin_num_before_sort = 0  # 此变量存放下架理库前此款的总箱数（排除单SKU整箱）
        have_full_bin_sku_num = 0
        total_full_bin_num = 0
        for sku in skus:
            amount, bin_num, full_bin_num = self.get_sku_total_item_num_exclude_full_bins(sku)
            if full_bin_num > 0:
                have_full_bin_sku_num += 1
            total_full_bin_num += full_bin_num
            total_bin_num_before_sort += bin_num
            assert (amount >= 0)
            if amount > 0:
                sku_amount.append([sku, amount])
                total_item_num += amount
        bins = self.b2b_sort(sku_amount, bin_capacity)
        if bins[-1].get_total_amount() == 0:
            bins.pop()
        total_bin_num_after_sort = len(bins)  # 此变量存放下架理库后此款的总箱数（排除单SKU整箱）
        if total_bin_num_before_sort == 0:
            mean_item_num_per_bin = 0
        else:
            mean_item_num_per_bin = total_item_num / total_bin_num_before_sort
        total_full_bin_item_num = bin_capacity * total_full_bin_num
        return total_bin_num_before_sort, total_item_num, mean_item_num_per_bin, total_bin_num_after_sort, \
               have_full_bin_sku_num, total_full_bin_num, total_full_bin_item_num

    # B2B 下架之前理库
    def b2b_sort_before_un_shelve(self):
        before_un_shelve_data = []  # 每款一行，款号，库存箱数（不包括单SKU整箱）,库存件数（不包括单SKU整箱）, 平均每箱分配件数
        style_num = 0  # 库存款数（不包括单SKU整箱）
        total_item_num = 0  # 总库存件数（不包括单SKU整箱）
        total_bin_num = 0  # 理货后总箱数（不包括单SKU整箱）
        total_have_full_bin_sku_num = 0
        total_full_bin_num = 0
        total_full_bin_item_num = 0
        style_to_skus = {}
        for sku, bin_amount in self.sku_bin_amount.items():
            style_code = get_style_code(sku)
            if not style_to_skus.__contains__(style_code):
                style_to_skus[style_code] = set()
            style_to_skus[style_code].add(sku)
        for style_code, skus in style_to_skus.items():
            bin_num_before_sort, item_num, mean_item_num_per_bin, bin_num_after_sort, \
            have_full_bin_sku_num, full_bin_num, full_bin_item_num = \
                self.b2b_sort_for_the_style(skus, 20)
            before_un_shelve_data.append([style_code, bin_num_before_sort, item_num, mean_item_num_per_bin])
            total_have_full_bin_sku_num += have_full_bin_sku_num
            total_full_bin_num += full_bin_num
            total_full_bin_item_num += full_bin_item_num
            if item_num > 0:
                style_num += 1
            total_item_num += item_num
            total_bin_num += bin_num_after_sort
        mean_item_num_per_bin_after_sort = total_item_num / total_bin_num
        return before_un_shelve_data, total_have_full_bin_sku_num, total_full_bin_num, total_full_bin_item_num, \
               style_num, total_item_num, mean_item_num_per_bin_after_sort
