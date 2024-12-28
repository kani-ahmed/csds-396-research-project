# models/fact_hospital.py
from extensions import db

class FactHospital(db.Model):
    __tablename__ = 'fact_hospitals'

    hospital_id = db.Column(db.Integer, primary_key=True)
    hospital_name = db.Column(db.String(255), nullable=False)
    location_id = db.Column(db.Integer, db.ForeignKey('fact_locations.location_id'), nullable=False)


    location = db.relationship('FactLocation', back_populates='hospitals')
    charges = db.relationship('FactHospitalCharge', back_populates='hospital')
