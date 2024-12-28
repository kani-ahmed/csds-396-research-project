# user_models.py

from datetime import datetime, timezone

from extensions import db


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)  # Indexed for faster lookups
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)  # For notifications, also indexed
    profile_picture = db.Column(db.String(255))  # URL to profile picture
    password_hash = db.Column(db.String(128))
