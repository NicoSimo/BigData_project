import csv
import os
import redis
import json
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def buildings_loader():
    redis_host = os.getenv('REDIS_HOST', 'redis')
    r = redis.Redis(host=redis_host, port=6379)
    with open("Setup/building_metadata_modified.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            key = str(row[1])
            value = {
                'primary use': row[2],
                'square feet': int(row[3]),
                'year_built': int(row[4]),
                'area': int(row[6])
            }
            # Use the Redis `set` command
            r.set(key, json.dumps(value))
    log.info(f"Building data loaded into Redis.")   

if __name__ == '__main__':
    buildings_loader()