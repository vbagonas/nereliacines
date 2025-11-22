from backend.mongas.db import MongoDB
from backend.redysas.ops import RedisClient
from backend.casa.kasandre import CassandraRepository
from backend.graph_db.graph import GraphDB

# Initialize but don't connect yet
db = MongoDB()
redis = RedisClient()
cassandra = CassandraRepository()
neo4 = GraphDB()
