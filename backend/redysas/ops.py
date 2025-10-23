import redis
import json
from datetime import timedelta
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class RedisClient:
    def __init__(self, ssl=True):
        self.host = 'redis-18424.c261.us-east-1-4.ec2.redns.redis-cloud.com'
        self.port = 18424
        self.ssl = ssl
        self.client = self.connect()

    def connect(self):
        with open(PROJECT_ROOT/'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
            creds = data.get('redis_user', data)

        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            decode_responses=True,
            password=creds['password'],
        )

        return self.client

    def test_connection(self):
        try:
            self.client.ping()
            print("Connected to Redis successfully!")
        except redis.exceptions.ConnectionError as e:
            print("Failed to connect:", e)

    def set_cache(self, key, value, ttl=None):
        """Save Python object (dict, list) to Redis as JSON with optional TTL."""
        try:
            value_json = json.dumps(value)
            if ttl:
                self.client.setex(key, timedelta(seconds=ttl), value_json)
            else:
                self.client.set(key, value_json)
        except Exception as e:
            print(f"Redis set_cache error: {e}")

    def get_cache(self, key):
        """Retrieve JSON value from Redis."""
        try:
            value = self.client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            print(f"Redis get_cache error: {e}")
            return None

    def invalidate_cache(self, key):
        """Delete cache key."""
        try:
            self.client.delete(key)
        except Exception as e:
            print(f"Redis invalidate error: {e}")

# Example usage
if __name__ == "__main__":
    red = RedisClient()
    red.connect()
    red.test_connection()
    red.client.set("foo", "bar")
    print(red.client.get("foo"))
