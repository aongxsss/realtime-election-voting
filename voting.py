import psycopg2
import random
import time
from datetime import datetime
import pendulum
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from main import delivery_report
import os
from dotenv import load_dotenv

load_dotenv()
host = os.getenv('host')
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')

conf = {
    'bootstrap.servers': 'localhost:9092'
}
consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

producer = SerializingProducer(conf)

def consume_messages():
    result = []
    consumer.subscribe(['candidates_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                result.append(json.loads(msg.value().decode('utf-8')))
                if len(result) == 3:
                    return result
    except KafkaException as e:
        print("Error: ", e)
if __name__ == "__main__":
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
    cur = conn.cursor()
    
    candidates_query = cur.execute("""
                                   SELECT row_to_json(t)
                                   FROM ( SELECT * FROM candidates ) t;
                                   """)
    candidates = cur.fetchall()
    # print(candidates)
    print("-"*50)
    candidates = [candidate[0] for candidate in candidates]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print("Number of candidates: ", len(candidates))
        for candidate in candidates:
            print(candidate['candidate_name'])
    
    consumer.subscribe(['voters_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    "voting_time": pendulum.now('UTC').format('YYYY-MM-DD HH:mm:ss'),
                    "vote":1
                }
            try:
                print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_name']}")
                cur.execute("""
                            INSERT INTO votes (voter_id, candidate_id, voting_time)
                            VALUES (%s, %s, %s)
                            """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                conn.commit()
                
                producer.produce(
                    'votes_topic',
                    key=vote['voter_id'],
                    value=json.dumps(vote),
                    on_delivery=delivery_report
                )
                producer.poll(0)
                time.sleep(0.2)
            except Exception as e:
                print(f"Error: {e}")
                continue
        # time.sleep(0.5)
    except KafkaError as e:
        print(f"Error: {e}")