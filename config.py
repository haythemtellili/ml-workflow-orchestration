import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
host = os.environ.get("HOST")
database = os.environ.get("POSTGRES_DB")
