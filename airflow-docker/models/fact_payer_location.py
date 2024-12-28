# models/fact_payer_location.py
from extensions import db

class FactPayerLocation(db.Model):
    __tablename__ = 'fact_payer_locations'

    id = db.Column(db.Integer, primary_key=True)
    payer_id = db.Column(db.Integer, db.ForeignKey('fact_payers.payer_id'), nullable=False)
    location_id = db.Column(db.Integer, db.ForeignKey('fact_locations.location_id'), nullable=False)
