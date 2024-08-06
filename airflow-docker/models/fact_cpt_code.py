# models/cpt_code.py
from extensions import db


class CPTCode(db.Model):
    __tablename__ = 'cpt_codes'

    cpt_id = db.Column(db.Integer, primary_key=True)
    cpt_code = db.Column(db.String(5), unique=True, nullable=False)

    charges = db.relationship('HospitalCharge', back_populates='cpt_code')