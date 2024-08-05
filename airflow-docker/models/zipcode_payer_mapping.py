# zipcode_payer.py

from extensions import db

ZipcodePayer = db.Table('Zipcodes_Payers',
                         db.Column('zipcode_id', db.Integer, db.ForeignKey('Zip_Codes.id'), primary_key=True),
                         db.Column('payer_id', db.Integer, db.ForeignKey('Payers.id'), primary_key=True))

