# models/fact_cpt_code.py
from extensions import db

class FactCPTCode(db.Model):
    __tablename__ = 'fact_cpt_codes'

    cpt_id = db.Column(db.Integer, primary_key=True)
    cpt_code = db.Column(db.Integer, unique=True, nullable=False)
    description = db.Column(db.Text, nullable=True)

    charges = db.relationship('FactHospitalCharge', back_populates='cpt_code')
    icd_mappings = db.relationship('FactICDCPTMapping', back_populates='cpt_code')
