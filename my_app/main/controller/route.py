from flask import request
from main import create_app
from .middleware import login_required
from .service import Service
from main.repository.postgresql import SQLRepository
from main import db
from main.model.item import Item, ItemSchema
from main.model.item_hierarchy import ItemHierarchy, ItemHierarchySchema
import json

repo_client = SQLRepository(db)
service = Service(repo_client)
app = create_app('dev')


@app.route('/items', methods=['GET'])
# @login_required
def show_all_items():
    return json_response(service.find_all(Item, ItemSchema))


@app.route('/item/<int:item_id>', methods=['GET'])
# @login_required
def show_item(item_id):
    item = service.find_by_id(Item, ItemSchema, item_id)
    if item:
        return json_response(item)
    else:
        return json_response({'error': 'item not found'}, 404)


@app.route('/item/<int:item_id>', methods=['PUT'])
# @login_required
def update_item(item_id):
    data = dict(json.loads(request.data))
    if not data:
        return json_response({'error': 'updated values were not provided'}, 422)

    if service.update_by_id(Item, data, item_id):
        return json_response(service.find_by_id(Item, ItemSchema, item_id))
    else:
        return json_response({'error': 'item not found'}, 404)


@app.route('/item_hierarchies', methods=['GET'])
# @login_required
def show_all_item_hierarchies():
    return json_response(service.find_all(ItemHierarchy, ItemHierarchySchema))


@app.route('/item_hierarchies', methods=['POST'])
# @login_required
def create_hierarchy():
    hierarchy = ItemHierarchySchema().load(json.loads(request.data))
    if not hierarchy:
        return json_response({'error': hierarchy.errors})

    return json_response(service.create_for(ItemHierarchySchema, hierarchy))


@app.route('/item_hierarchy/<int:item_hierarchy_id>', methods=['GET'])
# @login_required
def show_item_hierarchy(item_hierarchy_id):
    item_hierarchy = service.find_by_id(ItemHierarchy, ItemHierarchySchema, item_hierarchy_id)
    if item_hierarchy:
        return json_response(item_hierarchy)
    else:
        return json_response({'error': 'item hierarchy not found'}, 404)


@app.route('/item_hierarchy/<int:item_hierarchy_id>', methods=['PUT'])
# @login_required
def update_item_hierarchy(item_hierarchy_id):
    data = dict(json.loads(request.data))
    if not data:
        return json_response({'error': 'updated values were not provided'}, 422)

    if service.update_by_id(ItemHierarchy, data, item_hierarchy_id):
        return json_response(service.find_by_id(ItemHierarchy, ItemHierarchySchema, item_hierarchy_id))
    else:
        return json_response({'error': 'item not found'}, 404)


@app.route('/item_hierarchy/<int:item_hierarchy_id>', methods=['DELETE'])
# @login_required
def delete_item_hierarchy(item_hierarchy_id):
    hierarchy = service.find_by_id(ItemHierarchy, ItemHierarchySchema, item_hierarchy_id)
    if service.delete_by_id(ItemHierarchy, item_hierarchy_id):
        return json_response(hierarchy)
    else:
        return json_response({'error': 'item not found'}, 404)


def json_response(data, status=200):
    return json.dumps(data), status
