class SkuSeasonQuerier:
    def __init__(self):
        self.sku_to_season = {}

    def add_sku_season(self, sku, season):
        if self.sku_to_season.__contains__(sku):
            pass
        else:
            self.sku_to_season[sku] = season

    def get_sku_season(self, sku):
        return self.sku_to_season[sku]
