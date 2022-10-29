import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

user = os.environ.get("user")
password = os.environ.get("password")
host = os.environ.get("host")
database = os.environ.get("database")
