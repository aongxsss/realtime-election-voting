
import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
import os
from dotenv import load_dotenv
import time

load_dotenv()
host = os.getenv('host')
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')

URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Pheu Thai Party", "Move Forward Party", "Palang Pracharath Party"]
random.seed(66)

def generate_voter_data(existing_uuids=None):
   
    if existing_uuids is None:
        existing_uuids = set()
        
    max_attempts = 5  
    attempt = 0
    
    while attempt < max_attempts:
        try:
            res = requests.get(URL)
            if res.status_code == 200:
                user_data = res.json()['results'][0]
                uuid = user_data['login']['uuid']
                
                if uuid not in existing_uuids:
                    voter_data = {
                        "voter_id": uuid,
                        "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                        "date_of_birth": user_data['dob']['date'],
                        "gender": user_data['gender'],
                        "nationality": user_data['nat'],
                        "registration_number": user_data['login']['username'],
                        "address": {
                            "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                            "city": user_data['location']['city'],
                            "state": user_data['location']['state'],
                            "country": user_data['location']['country'],
                            "postcode": user_data['location']['postcode']
                        },
                        "email": user_data['email'],
                        "phone_number": user_data['phone'],
                        "cell_number": user_data['cell'],
                        "picture": user_data['picture']['large'],
                        "registered_age": user_data['registered']['age']
                    }
                    existing_uuids.add(uuid)
                    return voter_data
            else:
                print(f"API returned status code: {res.status_code}")
        except Exception as e:
            print("Error: ", e)
            
        attempt += 1
        time.sleep(0.1)
        
    return "Error fetching data"  
def generate_voter_data():
    res = requests.get(URL)
    if res.status_code == 200:
        user_data = res.json()['results'][0]
        # print(user_data['login']['uuid'])
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetcing data"
    
def create_candidate(candidate_id, candidate_name, party_affiliation,photo_url):
    return {
        "candidate_id": candidate_id,
        "candidate_name": candidate_name,
        "party_affiliation": party_affiliation,
        "photo_url": photo_url
    }
    
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'

def create_tables(conn, cur):
    cur.execute("""
               CREATE TABLE IF NOT EXISTS candidates (
                   candidate_id VARCHAR(255) PRIMARY KEY,
                   candidate_name VARCHAR(255),
                   party_affiliation VARCHAR(255),
                    photo_url TEXT
               )
               """)
    cur.execute("""
               CREATE TABLE IF NOT EXISTS voters (
                voter_id VARCHAR(255) PRIMARY KEY,
                voter_name VARCHAR(255),
                date_of_birth VARCHAR(255),
                gender VARCHAR(255),
                nationality VARCHAR(255),
                registration_number VARCHAR(255),
                address_street VARCHAR(255),
                address_city VARCHAR(255),
                address_state VARCHAR(255),
                address_country VARCHAR(255),
                address_postcode VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(255),
                cell_number VARCHAR(255),
                picture TEXT,
                registered_age INTEGER
               )
               """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    
    conn.commit()
    
def insert_voters(conn, cur, voter):
    cur.execute("""
                   INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()
    

if __name__ == "__main__":
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
    cur = conn.cursor()
    
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})   
    create_tables(conn, cur)
    
    cur.execute("""
                SELECT *
                FROM candidates
                """)
    candidates = cur.fetchall()
    if len(candidates) == 0:
        custom_candidates = [
        create_candidate(
            candidate_id="29",
            candidate_name="Srettha Thavisin",
            party_affiliation="Pheu Thai Party",
            photo_url="https://upload.wikimedia.org/wikipedia/commons/5/51/Srettha_Thavisin_at_Pheu_Thai_Party_headquarters%2CBangkok%2C_7_September_2023.jpg"
        ),
        create_candidate(
            candidate_id="31",
            candidate_name="Pita Limjaroenrat",
            party_affiliation="Move Forward Party",
            photo_url="https://ml1czqgskmun.i.optimole.com/w:auto/h:auto/q:mauto/f:avif/https://www.idd-brussels.eu/wp-content/uploads/2024/06/Pita-LIMJAROENRAT-scaled-e1723549221373.jpeg"
        ),
        create_candidate(
            candidate_id="37",
            candidate_name="Prawit Wongsuwan",
            party_affiliation="Palang Pracharath Party",
            photo_url="https://upload.wikimedia.org/wikipedia/commons/thumb/2/2b/Prawit_Wongsuwan_%282018%29_cropped.jpg/220px-Prawit_Wongsuwan_%282018%29_cropped.jpg"
        )
        ]
        for candidate in custom_candidates:
            cur.execute("""
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, photo_url)
                VALUES (%s, %s, %s, %s)
                """, (
                candidate['candidate_id'],
                candidate['candidate_name'],
                candidate['party_affiliation'],
                candidate['photo_url']
            ))
            conn.commit()
    # for i in range(5000):
    #     voter_data = generate_voter_data()
    #     insert_voters(conn, cur, voter_data)
    #     producer.produce(
    #         voters_topic,
    #         key=voter_data["voter_id"],
    #         value=json.dumps(voter_data),
    #         on_delivery=delivery_report
    #     )
    #     print('Produced voter {}, voter id: {}'.format(i, voter_data['voter_id']))
    #     producer.flush()
