# models/zipcode_data.py
from extensions import db


class ZipCode(db.Model):
    __tablename__ = 'zip_codes'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    code = db.Column(db.String(255), nullable=False, unique=True)
    city_id = db.Column(db.Integer, db.ForeignKey('cities.id'), nullable=False)
    hospitals = db.relationship('Hospital', backref='zipcode', lazy=True)
