import os
from pymongo import MongoClient


class MongoRepository(object):
    def __init__(self):
        mongo_url = os.environ.get('MONGO_URL')
        self.db = MongoClient(mongo_url).item

    def find_all(self, selector):
        return self.db.item.find(selector)

    def find(self, selector):
        return self.db.item.find_one(selector)

    def create(self, new_item):
        return self.db.item.insert_one(new_item)

    def update(self, selector, new_item):
        return self.db.kudos.replace_one(selector, new_item).modified_count

    def delete(self, selector):
        return self.db.item.delete_one(selector).deleted_count


if __name__ == '__main__':
    new_item = {"created_datetime": "2020-09-28T12:15:14.449212", "item_desc": "tomato from US california",
            "updated_datetime":
                "2020-09-28T12:15:14.449238", "pack_size": "1lb", "updated_by": None, "growing_method": "conventional",
            "hierarchy":
                {"created_datetime": "2020-09-28T12:41:06.045648", "commodity_group": "tomato",
                 "subcommodity": "tomato",
                 "updated_datetime": "2020-09-28T16:13:38.396786", "variety": "baby tomato", "updated_by": "kloojin",
                 "strategy_group":
                     "strategic", "commodity": "tomato", "created_by": "test_user"}, "created_by": None}
    mongo_client = MongoRepository()
    mongo_client.create(
        new_item
    )
