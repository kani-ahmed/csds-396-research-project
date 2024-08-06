# models/payer_location.py
from extensions import db


class PayerLocation(db.Model):
    __tablename__ = 'payer_locations'

    id = db.Column(db.Integer, primary_key=True)
    payer_id = db.Column(db.Integer, db.ForeignKey('payers.payer_id'), nullable=False)
    location_id = db.Column(db.Integer, db.ForeignKey('locations.location_id'), nullable=False)