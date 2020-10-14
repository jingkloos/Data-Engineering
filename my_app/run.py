from main import create_app
from main.controller.route import app
from main.repository.postgresql import SQLRepository
from main.model.item_hierarchy import ItemHierarchy, ItemHierarchySchema
from main.model.item import Item, ItemSchema
from main.controller.service import Service
import json

from datetime import datetime
from pprint import pprint

app.run(debug=True)
# def json_response(data, status=200):
#     payload = {'status': status,
#                'values': data}
#     return json.dumps(payload)
# svc=Service(SQLRepository(db))
# res=svc.find_all(Item, ItemSchema)
#
# print(json_response(res))
# db.drop_all()
#
# db.create_all()
# item_schema = ItemSchema()
# item_hierarchy_schema = ItemHierarchySchema()

# new_item_hierarchy = ItemHierarchy(variety='cheery tomato', subcommodity='tomato', commodity_group='tomato'
#                                    , commodity='tomato', strategy_group='strategic', created_by='test_user')
# item = Item(item_desc='tomato from US california', pack_size='1lb', growing_method='organic')
# repo = SQLRepository(db)
# res = repo.show_by_expression(Item, expr=Item.hierarchy_id.is_(None), nrows=5)
# se_res=item_hierarchy_schema.dump(res, many=True)
# print(res)
# hierarchy = ItemHierarchy.find_by_id(1)
# repo.create(new_item_hierarchy)
# repo.create(item)
# ct = repo.update(Item, {'id': 2}, {'item_desc':'organic '+Item.item_desc})
# print('updated row count is', ct)
# item_hierarchy = ItemHierarchy.find_by_id(1)
# print(ItemHierarchy.find_all())
# pprint(hierarchy.items)
