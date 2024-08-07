import requests
import json
import time
from faker import Faker
import random

faker = Faker()

url = 'http://predictor.ai/api/predict'
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

def generate_fake_transaction():
    return {
        "transaction_id": faker.random_number(digits=7, fix_len=True),
        "tx_datetime": faker.date_time_this_decade().isoformat(),
        "customer_id": faker.random_number(digits=4, fix_len=True),
        "terminal_id": faker.random_number(digits=3, fix_len=True),
        "tx_amount": round(random.uniform(1.0, 1000.0), 2),
        "tx_time_seconds": random.randint(1, 1000000),
        "tx_time_days": random.randint(1, 365)
    }


end_time = time.time() + 30
while time.time() < end_time:

    data = [generate_fake_transaction() for _ in range(3)]
    data_json = json.dumps(data)

    response = requests.post(url, headers=headers, data=data_json)

    print(response.status_code)
    if response.status_code == 200:
        print(response.json())
    else:
        print(response.text)
    
    time.sleep(0.01)

print("Finished sending requests.")
