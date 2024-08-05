from extensions import db

class StagingTable(db.Model):
    __tablename__ = 'staging_table'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cpt_code = db.Column(db.String(255), nullable=True)
    cash_discount = db.Column(db.Numeric(10, 2), nullable=True)
    deidentified_max_allowed = db.Column(db.Numeric(10, 2), nullable=True)
    deidentified_min_allowed = db.Column(db.Numeric(10, 2), nullable=True)
    payer_id = db.Column(db.String(255), nullable=True)
    city_id = db.Column(db.String(255), nullable=True)
    zipcode_id = db.Column(db.String(255), nullable=True)
    hospital_id = db.Column(db.String(255), nullable=True)
    description = db.Column(db.String(255), nullable=True)
    gross_charge = db.Column(db.Numeric(10, 2), nullable=True)
    payer_allowed_amount = db.Column(db.Numeric(10, 2), nullable=True)