from extensions import db

class FacilityDetails(db.Model):
    __tablename__ = 'facility_details'

    facility_id = db.Column(db.Integer, primary_key=True)
    facility_name = db.Column(db.String(255), unique=True, nullable=False)
    facility_type = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(255), nullable=True)
    city = db.Column(db.String(255), nullable=True)
    state = db.Column(db.String(255), nullable=True)
    zip_code = db.Column(db.String(255), nullable=True)
    phone = db.Column(db.String(255), nullable=True)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    ccn = db.Column(db.String(255), nullable=True)
    beds = db.Column(db.Integer, nullable=True)
