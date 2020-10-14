from marshmallow import post_load

from main import db, ma
from datetime import datetime


class Item(db.Model):
    __tablename__ = 'item'
    __table_args__ = {'schema': 'dw'}
    id = db.Column(db.Integer, primary_key=True)
    item_desc = db.Column(db.String(500), nullable=False)
    pack_size = db.Column(db.String(50))
    growing_method = db.Column(db.String(50), nullable=False, default='conventional')
    # foreign key should use the actual table name with schema
    hierarchy_id = db.Column(db.Integer, db.ForeignKey('dw.item_hierarchy.id'))
    created_by = db.Column(db.String(50))
    updated_by = db.Column(db.String(50))
    created_datetime = db.Column(db.DateTime, nullable=False, default=datetime.now())
    updated_datetime = db.Column(db.DateTime, nullable=False, default=datetime.now())
    # hierarchy = db.relationship('ItemHierarchy', backref=db.backref('items', lazy=True))

    def __repr__(self):
        return '<Item %r>' % self.item_desc


class ItemSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Item
        include_fk = True

    @post_load
    def make_item(self, data, **kwargs):
        return Item(**data)
