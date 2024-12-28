# models/fact_location.py
from extensions import db

class FactLocation(db.Model):
    __tablename__ = 'fact_locations'

    location_id = db.Column(db.Integer, primary_key=True)
    state = db.Column(db.String(2), nullable=False)
    city = db.Column(db.String(100), nullable=False)
    zipcode = db.Column(db.String(10), nullable=False)

    hospitals = db.relationship('FactHospital', back_populates='location')
    payers = db.relationship('FactPayer', secondary='fact_payer_locations', back_populates='locations')
    charges = db.relationship('FactHospitalCharge', back_populates='location')
