from neo4j import GraphDatabase
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class GraphDB():
    def __init__(self):
        self.driver = self.connect()
        self.session = self.driver.session()

    def connect(self):
        with open(PROJECT_ROOT/'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
        creds = data.get('mongo_user', data)

        driver = GraphDatabase.driver(creds['uri'], auth=(creds['user'], creds['password']))
        return driver