# models/fact_icd_cpt_mapping.py
from extensions import db

class FactICDCPTMapping(db.Model):
    __tablename__ = 'fact_icd_cpt_mappings'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    icd_code = db.Column(db.String(8), nullable=False)  # ICD-10-CM codes are typically 3-7 characters
    cpt_id = db.Column(db.Integer, db.ForeignKey('fact_cpt_codes.cpt_id'), nullable=False)
    icd_description = db.Column(db.Text, nullable=True)

    # Relationship with the FactCPTCode model
    cpt_code = db.relationship('FactCPTCode', back_populates='icd_mappings')