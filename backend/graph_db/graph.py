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
    
    def close(self):
        """Close connection"""
        self.session.close()
        self.driver.close()
    
    def _run_query(self, query, params=None):
        """Execute query and return results"""
        result = self.session.run(query, params or {})
        return [record.data() for record in result]

    def add_event(self, event_id, event_date):
        """Create/update event with date"""
        query = """
            MERGE (e:Event {id: $event_id})
            SET e.date = datetime($event_date)
        """
        return self._run_query(query, {'event_id': event_id, 'event_date': event_date})


    def add_purchase(self, user_id, event_id, event_date=None):
        """Record purchase and optionally set event date"""
        query = """
            MERGE (u:User {id: $user_id})
            MERGE (e:Event {id: $event_id})
            MERGE (u)-[p:PURCHASED]->(e)
            ON CREATE SET p.timestamp = timestamp()
        """
        if event_date:
            query += "SET e.date = datetime($event_date)"
        
        return self._run_query(query, {
            'user_id': user_id, 
            'event_id': event_id,
            'event_date': event_date
        })


    def recommend_collaborative(self, user_id, limit=5):
        """All recommended events based on similar users"""
        query = """
            MATCH (u:User {id: $user_id})-[:PURCHASED]->(e:Event)
            MATCH (e)<-[:PURCHASED]-(similar:User)-[:PURCHASED]->(rec:Event)
            WHERE NOT (u)-[:PURCHASED]->(rec)
            RETURN rec.id as event_id, 
                rec.date as event_date,
                count(DISTINCT similar) as score
            ORDER BY score DESC
            LIMIT $limit
        """
        return self._run_query(query, {'user_id': user_id, 'limit': limit})
    

    def recommend_collaborative_upcoming(self, user_id, limit=5, months_ahead=2):
        """Upcoming recommended events only"""
        query = """
        MATCH (u:User {id: $user_id})-[:PURCHASED]->(e:Event)
        MATCH (e)<-[:PURCHASED]-(similar:User)-[:PURCHASED]->(rec:Event)
        WHERE NOT (u)-[:PURCHASED]->(rec)
          AND rec.date >= datetime() 
          AND rec.date <= datetime() + duration({months: $months_ahead})
        RETURN rec.id as event_id, 
               rec.date as event_date,
               count(DISTINCT similar) as score
        ORDER BY score DESC
        LIMIT $limit
        """
        return self._run_query(query, {
            'user_id': user_id, 
            'limit': limit,
            'months_ahead': months_ahead
        })