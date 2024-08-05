# models/icd_cpt_mappings.py
from extensions import db

class IcdCptMapping(db.Model):
    __tablename__ = 'icd_cpt_mappings'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    icd_code = db.Column(db.String(255), nullable=False)
    cpt_code = db.Column(db.String(255), db.ForeignKey('cpt_translations.cpt_code'), nullable=False)

    cpt_translation = db.relationship('CptTranslation', backref=db.backref('icd_cpt_mappings', lazy='dynamic'))