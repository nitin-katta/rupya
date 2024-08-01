from mftool import Mftool
import pandas as pd
import time
import json
import logging
import mysql.connector
from mysql.connector import Error
import schedule
from datetime import datetime, timedelta
import pytz
from db import create_database_connection
import requests
import os
from requests.exceptions import RequestException
from backend import db


current_date = datetime.now().strftime('%Y-%m-%d')
log_file_name = f'logs/mutual_fund_data_fetch_{current_date}.log'
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename=log_file_name)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

logger = logging.getLogger(__name__)


class mutual_fund(db.Model):
    __tablename__ = 'mutual_funds'
    fund_id = db.Column(db.Integer, primary_key=True,autoincrement=True, nullable = False)
    fund_name = db.Column(db.String(250),nullable = False)
    fund_code = db.Column(db.String(20),unique=True, nullable=False)
    category =  db.Column(db.String(50))
    current_nav = db.Column(db.Decimal(10,2))
    last_updated = db.Column(db.db.DateTime, default=db.func.current_timestamp())




