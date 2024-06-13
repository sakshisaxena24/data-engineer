from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'sakshi',
    'start_date': datetime(2024, 6, 9, 10, 00)
}


def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]  # get only 1st index
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


def stream_data():
    from kafka import KafkaProducer
    import json
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        res = get_data()
        res = format_data(res)
        logger.info(f"Formatted data: {res}")

        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        future = producer.send('user_created', res)
        result = future.get(timeout=10)
        logger.info(f"Message sent: {result}")

    except Exception as e:
        logger.error(f"Failed to send message: {e}")


# DAG entry point
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data  # the function that will be called
    )

# Ensure no direct call to stream_data() outside the task definition
# stream_data()  # This line should remain commented out
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'sakshi',
    'start_date': datetime(2024, 6, 9, 10, 00)
}


def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]  # get only 1st index
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


def stream_data():
    from kafka import KafkaProducer
    import json
    import logging
    import time

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)



    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #stream for 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            logger.info(f"Formatted data: {res}")
            producer.send('users_created',json.dumps(res).encode('utf-8'))

        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            continue


# DAG entry point
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data  # the function that will be called
    )

