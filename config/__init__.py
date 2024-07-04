import os
from dotenv import load_dotenv

import configparser

# Load the environment variables from the .env file
load_dotenv()

# Load the configuration file
config = configparser.ConfigParser()
config.read(filenames=os.path.join('config', os.getenv(key='MODE') + '.conf'))

ORCHESTRATION_ADDRESS = config.get(section='orchestration', option='address')
MONGODB_ADDRESS = config.get(section='mongodb', option='address')

MINIO_ADDRESS = config.get(section='minio', option='address')
MINIO_ACCESS_KEY = os.getenv(key='MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv(key='MINIO_SECRET_KEY')