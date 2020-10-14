class SQLRepository(object):
    def __init__(self, db):
        self.db = db

    def create(self, item):
        """
        :param item: an object that will be inserted to db
        :return: None
        """
        self.db.session.add(item)
        self.db.session.commit()

    def delete(self, model, selector):
        """
        :param model: model class
        :param selector: **kwargs
        :return: int deleted row count
        """
        rows = self.db.session.query(model).filter_by(**selector).delete()
        self.db.session.commit()
        return rows

    def update(self, model, selector, updater):
        """
        :param updater: dictionary
        :param model: model class
        :param selector: **kwargs
        :return: int updated row count
        """
        rows = self.db.session.query(model).filter_by(**selector).update(updater, synchronize_session=False)
        self.db.session.commit()
        return rows

    def find(self, model, selector=None, nrows=None):
        """
        return rows in a table based on selector, limit to nrows
        :param nrows: int
        :param model: model class
        :param selector: **kwargs
        :return: list
        """
        if selector and nrows:
            return self.db.session.query(model).filter_by(**selector).limit(nrows).all()

        if selector:
            return self.db.session.query(model).filter_by(**selector).all()

        if nrows:
            return self.db.session.query(model).limit(nrows).all()

        return self.db.session.query(model).all()

    def find_by_expression(self, model, expr, nrows=None):
        """
        return rows in a table based on selector, limit to nrows
        :param expr: expression to filter
        :param nrows: int
        :param model: model class
        :return: list
        """
        if nrows:
            return self.db.session.query(model).filter(expr).limit(nrows).all()

        return self.db.session.query(model).filter(expr).all()
