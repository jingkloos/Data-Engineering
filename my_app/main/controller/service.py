# translate API request payload to database object


class Service(object):
    def __init__(self, repo_client):
        self.repo_client = repo_client

    def _convert(self, schema, data):
        return schema(exclude=['id']).dump(data)

    def find_all(self, model, schema):
        items = self.repo_client.find(model)
        return [self._convert(schema, item) for item in items]

    def find_by_id(self, model, schema, item_id):
        item = self.repo_client.find(model, {'id': item_id})
        return self._convert(schema, item[0])

    def create_for(self, schema, data):
        """
        :param schema: schema
        :param data: model object
        :return:
        """
        self.repo_client.create(data)
        return self._convert(schema, data)

    def update_by_id(self, model, data, item_id):
        """
        :param model: model class
        :param data: dictionary that has update values
        :param item_id: int
        :return: bool if any records were updated
        """
        records_affected = self.repo_client.update(model, {'id': item_id}, data)
        return records_affected > 0

    def delete_by_id(self, model, item_id):
        records_affected = self.repo_client.delete(model, {'id': item_id})
        return records_affected > 0
