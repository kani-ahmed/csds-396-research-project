# models/hospital.py
from extensions import db


class Hospital(db.Model):
    __tablename__ = 'hospitals'

    hospital_id = db.Column(db.Integer, primary_key=True)
    hospital = db.Column(db.String(255), nullable=False)
    location_id = db.Column(db.Integer, db.ForeignKey('locations.location_id'), nullable=False)

    location = db.relationship('Location', back_populates='hospitals')
    charges = db.relationship('HospitalCharge', back_populates='hospital')