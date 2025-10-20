from pathlib import Path
import yaml
from pymongo import MongoClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class MongoDB():
    def __init__(self):
        self.client = self.connect()
        self.db = self.client['eVent'] 
        self.vartotojai = self.db["Vartotojai"]
        self.renginiai = self.db["Renginiai"]
        self.uzsakymai = self.db["Užsakymai"] 

    def connect(self):
        if self.client is None:
            with open(PROJECT_ROOT/'creds.yml', 'r') as f:
                data = yaml.safe_load(f) or {}
            creds = data.get('mongo_user', data)
            self.client = MongoClient(f"mongodb+srv://{creds['username']}:{creds['password']}@manomb.gi8bjhg.mongodb.net/")

        return self.client




