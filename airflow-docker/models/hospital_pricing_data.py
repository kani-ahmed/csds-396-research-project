# hospital_charge model in models/hospital_charge.py

from extensions import db


class HospitalPricingData(db.Model):
    __tablename__ = 'Hospital_Pricing_Data'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    Associated_Codes = db.Column(db.String(255), db.ForeignKey('CPT_Translations.CPT_Code'), nullable=False)
    Cash_Discount = db.Column(db.Numeric(10, 2), nullable=True)
    Deidentified_Max_Allowed = db.Column(db.String(255), nullable=True)
    Deidentified_Min_Allowed = db.Column(db.Numeric(10, 2), nullable=True)
    payer_id = db.Column(db.Integer, db.ForeignKey('Payers.id'), nullable=True)
    city_id = db.Column(db.Integer, db.ForeignKey('Cities.id'), nullable=False)
    zipcode_id = db.Column(db.Integer, db.ForeignKey('Zip_Codes.id'), nullable=False)
    hospital_id = db.Column(db.Integer, db.ForeignKey('Hospitals.id'), nullable=False)

    cpt_translation = db.relationship('CptTranslation', backref='hospital_charges')
    payer = db.relationship('Payer', backref='hospital_charges')
    city = db.relationship('City', backref='hospital_charges')
    zipcode = db.relationship('ZipCode', backref='hospital_charges')
    hospital = db.relationship('Hospital', backref='hospital_charges')
