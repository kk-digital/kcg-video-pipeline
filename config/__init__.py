import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Load the configuration file

# orchestration
ORCHESTRATION_ADDRESS = os.getenv(key="ORCHESTRATION_ADDRESS")

# mongodb
MONGODB_ADDRESS = os.getenv(key="MODE")

# minio
MINIO_ADDRESS = os.getenv(key="MINIO_ADDRESS")
MINIO_ACCESS_KEY = os.getenv(key="MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv(key="MINIO_SECRET_KEY")
