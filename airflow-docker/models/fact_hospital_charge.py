# models/fact_hospital_charge.py
from extensions import db

class FactHospitalCharge(db.Model):
    __tablename__ = 'fact_hospital_charges'

    id = db.Column(db.Integer, primary_key=True)
    cpt_id = db.Column(db.Integer, db.ForeignKey('fact_cpt_codes.cpt_id'), nullable=False)
    payer_id = db.Column(db.Integer, db.ForeignKey('fact_payers.payer_id'), nullable=False)
    hospital_id = db.Column(db.Integer, db.ForeignKey('fact_hospitals.hospital_id'), nullable=False)
    location_id = db.Column(db.Integer, db.ForeignKey('fact_locations.location_id'), nullable=False)
    cash_discount = db.Column(db.Float)
    deidentified_max_allowed = db.Column(db.Float)
    deidentified_min_allowed = db.Column(db.Float)
    description = db.Column(db.Text)
    gross_charge = db.Column(db.Float)
    payer_allowed_amount = db.Column(db.Float)

    cpt_code = db.relationship('FactCPTCode', back_populates='charges')
    payer = db.relationship('FactPayer', back_populates='charges')
    hospital = db.relationship('FactHospital', back_populates='charges')
    location = db.relationship('FactLocation', back_populates='charges')
