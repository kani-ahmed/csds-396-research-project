# models/location.py
from extensions import db


class Location(db.Model):
    __tablename__ = 'locations'

    location_id = db.Column(db.Integer, primary_key=True)
    state = db.Column(db.String(2), nullable=False)
    city = db.Column(db.String(100), nullable=False)
    zipcode = db.Column(db.String(10), nullable=False)

    hospitals = db.relationship('Hospital', back_populates='location')
    payers = db.relationship('Payer', secondary='payer_locations', back_populates='locations')
    charges = db.relationship('HospitalCharge', back_populates='location')