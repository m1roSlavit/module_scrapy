from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from sqlite3 import connect
from ekatalog.items import EkatalogItem


class EkatalogPipeline:
    def process_item(self, item, spider):
        return item

class SaveToDbPipline:
    def open_spider(self, spider):
        self.connection = connect("ekatalog.db")
        self.cursor = self.connection.cursor()

    def process_item(self, item, spider):
        if isinstance(item, EkatalogItem):
            for laptop in self.cursor.execute(
                "SELECT id, model FROM projectors WHERE model=?",
                [item["model"]]
            ):
                spider.logger.info(f"{item['model']} is in db, updating price")
                self.cursor.execute(
                    "UPDATE projectors SET price=?, priceUSD=? WHERE id=?",
                    [item["price"], item["priceUSD"], laptop[0]]
                )
                self.connection.commit()
                return item
            self.cursor.execute(
                "INSERT INTO projectors (model, model_url, shops, start_price, end_price, img_url) VALUES (?,?,?,?,?,?,?)",
                [item["model"], item["model_url"], " ".join(item["shops"]),
                    item["start_price"], item["end_price"], item["img_url"]]
            )
            self.connection.commit()
        return item

    def close_spider(self, spider):
        self.connection.close()