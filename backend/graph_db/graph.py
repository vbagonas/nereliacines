from neo4j import GraphDatabase
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class GraphDB():
    def __init__(self):
        self.driver = self.connect()
        self.session = self.driver.session(database="neo4j")
        print("✅ Connected to Neo4j database")

    def connect(self):
        with open(PROJECT_ROOT/'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
        creds = data.get('neo4j_user', data)

        driver = GraphDatabase.driver(creds['uri'], auth=(creds['username'], creds['password']))
        return driver