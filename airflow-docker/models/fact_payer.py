# models/payer.py
from extensions import db


class Payer(db.Model):
    __tablename__ = 'payers'

    payer_id = db.Column(db.Integer, primary_key=True)
    payer = db.Column(db.String(255), nullable=False)

    locations = db.relationship('Location', secondary='payer_locations', back_populates='payers')
    charges = db.relationship('HospitalCharge', back_populates='payer')