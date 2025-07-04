import os
from dotenv import load_dotenv
from airflow.models import Variable

# Load .env for local development only
ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
if ENVIRONMENT != "production":
    # Adjust path if config.py is in Configs/
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_config(key):
    if ENVIRONMENT == "production":
        # Fetch from Airflow Variables
        return Variable.get(key)
    else:
        # Fetch from .env or OS environment
        return os.getenv(key)

enviroment_dataset = get_config("ENVIRONMENT_DATASET")
table_log = get_config("TABLE_LOG")
table_config = get_config("TABLE_CONFIG")
system_params = get_config("SYSTEM_PARAMS")
fire_url = get_config("FIRE_URL")