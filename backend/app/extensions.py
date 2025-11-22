from backend.mongas.db import MongoDB
from backend.redysas.ops import RedisClient
from backend.casa.kasandre import CassandraRepository

# Initialize but don't connect yet
db = MongoDB()
redis = RedisClient()
cassandra = CassandraRepository()