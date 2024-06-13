from kafka import KafkaConsumer
import json
import redis

# Kafka setup
topic_name = 'energy_consumption_topic'

# Setting up the Kafka consumer
consumer = KafkaConsumer(
    topic_name, # The topic to consume messages from
    bootstrap_servers=['localhost:9094'], # The Kafka cluster's address
    auto_offset_reset='earliest', 
    group_id='redis-group' # The consumer group ID
)

# Redis connection setup --- port MUST be the same as the one in the docker-compose.yml file
r = redis.Redis(host='localhost', port=6379, db=0)

##########################################################################################################################################
# To retrieve the data from the Redis database, you can use the following commands:
# KEYS *: Returns all the keys in the database.
# GET 'key' : Returns the value stored at the key.
# LRANGE 'key' 0 -1: Returns all the elements in the LIST stored at the key.

# Considering we are working with a Time series, we are going to use A LIST to store the data. So use LRANGE.
##########################################################################################################################################


def ensure_list_key(key):
    '''
    This function checks the type of the key. If it exists and is not a list, it deletes the key to avoid the WRONGTYPE error.
    '''
    try:
        if r.type(key) != b'none' and r.type(key) != b'list':
            r.delete(key)
    except redis.ResponseError:
        r.delete(key)

# Consume messages
for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    key = f"building:{data['building_id']}"
    measurement = json.dumps({
        "timestamp": data['timestamp'],
        "meter_reading": data['meter_reading']
    })

    # Ensure the key is a list
    ensure_list_key(key)

    # LPUSH: Pushes a new element to the beginning of the list at key.
    # LTRIM: Trims the list to ensure only the last 5 entries are kept. This operation is efficient and ensures the list only contains the most recent 5 updates.
    # LRANGE: Retrieves the latest 5 entries from the list at key. This operation is optional and can be used for debugging purposes.
    
    r.lpush(key, measurement)
    r.ltrim(key, 0, 4)  # Keeps the latest 5 entries in the list (0-4) -----> 5 elements needs to be aligned with the TTL.
    # In here instead of setting a TTL, we are using the LTRIM to keep the latest 5 entries. It is done to simulate the behaviour of the TTL while the code is still under change.

    # Optionally print the current list
    #print(f"Updated Redis list for {key}: {r.lrange(key, 0, -1)}")  