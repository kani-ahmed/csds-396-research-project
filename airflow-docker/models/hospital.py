# models/hospitals_data.py
from extensions import db

class Hospital(db.Model):
    __tablename__ = 'hospitals'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    zipcode_id = db.Column(db.Integer, db.ForeignKey('zip_codes.id'), nullable=False)

