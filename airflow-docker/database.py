# database.py
from extensions import db, create_app

def get_db():
    app = create_app()
    with app.app_context():
        yield db.session