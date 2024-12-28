# models/fact_payer.py
from extensions import db

class FactPayer(db.Model):
    __tablename__ = 'fact_payers'

    payer_id = db.Column(db.Integer, primary_key=True)
    payer = db.Column(db.String(255), nullable=False)

    locations = db.relationship('FactLocation', secondary='fact_payer_locations', back_populates='payers')
    charges = db.relationship('FactHospitalCharge', back_populates='payer')
