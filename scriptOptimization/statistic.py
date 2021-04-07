

def statistic_outbound_data(matching_sale_lines):  # SaleOrderMatchingLine
    bin_sku_dict = {}
    bin_amount_dict = {}
    for line in matching_sale_lines:
        if bin_sku_dict.__contains__(line.bin_id):
            bin_sku_dict[line.bin_id].add(line.sku)
        else:
            bin_sku_dict[line.bin_id] = {line.sku}
        if bin_amount_dict.__contains__(line.bin_id):
            bin_amount_dict[line.bin_id] += line.amount
        else:
            bin_amount_dict[line.bin_id] = line.amount
    total_bin_num = len(bin_sku_dict)
    if total_bin_num == 0:
        return 0, 0, 0, 0
    total_sku_num = 0
    total_item_amount = 0
    for bin_id, sku_set in bin_sku_dict.items():
        total_sku_num += len(sku_set)
    for bin_id, amount in bin_amount_dict.items():
        total_item_amount += amount
    return total_sku_num / total_bin_num, total_item_amount / total_bin_num, total_bin_num, total_item_amount
