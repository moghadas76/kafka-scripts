
import json
import ssl
import sys
from kafka import KafkaConsumer
import logging
import os
import dotenv
from pathlib import Path

dotenv.load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
file_hd = logging.FileHandler("./consumer.log", mode="a")
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(file_hd)

logger.info('os.environ["SASL_USERNAME"]: ' + os.environ["SASL_USERNAME"])
conf = {
    'bootstrap.servers': os.environ["BOOTSTRAP_SERVER"],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ["SASL_USERNAME"],
    'sasl.password': os.environ["SASL_PASSWORD"]
}
sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

TARGET_DIR = Path(os.environ["TARGET_DIR"])
TARGET_DIR.mkdir(parents=True, exist_ok=True)

conf = {
    'security_protocol': 'SASL_SSL',
    'sasl_mechanisms': 'PLAIN',
    'sasl_username': os.environ["SASL_USERNAME"],
    'sasl_password': os.environ["SASL_PASSWORD"],
    # 'auto.offset.reset': 'earliest',  # Start from the beginning of the topic
    'receive.message.max.bytes': 1500000000000000000,
}

# consumer = Consumer(conf)
consumer = KafkaConsumer('topic_name', bootstrap_servers=[os.environ["BOOTSTRAP_SERVER"]],
                         sasl_plain_username = conf["sasl_username"],
                         sasl_plain_password=conf["sasl_password"],
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         api_version = (0,10),
                         fetch_max_bytes=5000
                         )
from datetime import datetime

# consumer.seek_to_end()
for msg in consumer:
    logger.info(f"msg.timestamp: {msg.timestamp}")
    dt = datetime.fromtimestamp(msg.timestamp / 1000)
    message_dir = TARGET_DIR / str(dt.year) / str(dt.month)/ str(dt.day)
    message_dir.mkdir(parents=True, exist_ok=True)
    with open((message_dir / f"raw_data.txt").as_posix(), "ab") as x:
        x.write(msg.value)
