from marshmallow import post_load

from main import db, ma
from datetime import datetime


class ItemHierarchy(db.Model):
    __tablename__ = 'item_hierarchy'
    __table_args__ = {'schema': 'dw'}
    id = db.Column(db.Integer, primary_key=True)
    variety = db.Column(db.String(50), unique=True, nullable=False)
    subcommodity = db.Column(db.String(50))
    commodity = db.Column(db.String(50))
    commodity_group = db.Column(db.String(50))
    strategy_group = db.Column(db.String(50))
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_datetime = db.Column(db.DateTime, nullable=False, default=datetime.now())
    updated_datetime = db.Column(db.DateTime, nullable=False, default=datetime.now())

    def __repr__(self):
        return '<ItemHierarchy: %r>' % self.variety


class ItemHierarchySchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = ItemHierarchy

    @post_load
    def make_item_hierarchy(self, data, **kwargs):
        return ItemHierarchy(**data)
