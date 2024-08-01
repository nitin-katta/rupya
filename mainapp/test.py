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

if not os.path.exists('logs'):
    os.makedirs('logs')

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

def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mutual_funds (
            fund_id INT AUTO_INCREMENT PRIMARY KEY,
            fund_name VARCHAR(100) NOT NULL,
            fund_code VARCHAR(20) UNIQUE NOT NULL,
            category VARCHAR(100),
            current_nav DECIMAL(10, 2),
            last_updated TIMESTAMP
        )
    """)

def get_existing_scheme_codes(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT fund_code FROM mutual_funds")
    scheme_codes = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return scheme_codes

def insert_or_update_fund(connection, fund_data_batch):
    cursor = connection.cursor()
    successful_updates = 0
    failed_updates = 0

    try:
        query = """
        INSERT INTO mutual_funds (fund_name, fund_code, category, current_nav, last_updated)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        fund_name = VALUES(fund_name),
        category = VALUES(category),
        current_nav = VALUES(current_nav),
        last_updated = VALUES(last_updated)
        """
        
        cursor.executemany(query, fund_data_batch)
        successful_updates = cursor.rowcount
        connection.commit()
        logger.info(f"Successfully updated {successful_updates} funds.")
    except mysql.connector.Error as e:
        connection.rollback()
        logger.error(f"Transaction failed. Error: {e}")
        failed_updates = len(fund_data_batch)
    finally:
        cursor.close()

    return successful_updates, failed_updates

def update_fund_nav(cursor, fund_data_batch):
    query = """
    UPDATE mutual_funds
    SET current_nav = %s, last_updated = %s
    WHERE fund_code = %s
    """
    cursor.executemany(query, fund_data_batch)


def fetch_mutual_fund_nav_data(mf, scheme_codes):
    data = []
    total_schemes = len(scheme_codes)
    
    for index, scheme_code in enumerate(scheme_codes, 1):
        try:
            nav_details = mf.get_scheme_quote(scheme_code)
            
            current_nav = nav_details.get('nav')
            last_updated = nav_details.get("last_updated")
            
            if current_nav and last_updated:
                data.append((
                    None, 
                    scheme_code,
                    None,  
                    float(current_nav),
                    last_updated
                ))
                logger.info(f"Fetched NAV {current_nav} for scheme {scheme_code}")
            else:
                logger.warning(f"Incomplete data for scheme {scheme_code}")
            
            if index % 100 == 0:
                logger.info(f"Processed {index}/{total_schemes} schemes")
                time.sleep(1) 
        except Exception as e:
            logger.error(f"Error processing scheme {scheme_code}: {str(e)}", exc_info=True)
    
    return data

def fetch_mutual_fund_full_data(mf, scheme_codes):
    data = []
    total_schemes = len(scheme_codes)
    
    for index, scheme_code in enumerate(scheme_codes, 1):
        try:
            logger.info(f"Processing scheme {scheme_code} ({index}/{total_schemes})")
            
            nav_details = mf.get_scheme_quote(scheme_code)
            asset_category = mf.get_scheme_details(scheme_code)
            
            if not isinstance(nav_details, dict) or not isinstance(asset_category, dict):
                raise ValueError(f"Unexpected response format for scheme {scheme_code}")
            
            scheme_name = nav_details.get('scheme_name')
            current_nav = nav_details.get('nav')
            last_updated = nav_details.get("last_updated")
            category_type = asset_category.get('scheme_category')
            
            if scheme_name and last_updated and category_type:
                if current_nav is None:
                    current_nav = 'Not Available'
                    
                last_updated = datetime.strptime(last_updated, '%d-%b-%Y')
                last_updated = last_updated.strftime('%Y-%m-%d %H:%M:%S')
                
                data.append((
                    scheme_name,
                    scheme_code,
                    category_type,
                    current_nav if isinstance(current_nav, str) else float(current_nav),
                    last_updated
                ))
                logger.info(f"Successfully processed scheme {scheme_code}")
            else:
                logger.warning(f"Incomplete data for scheme {scheme_code}. scheme_name: {scheme_name}, last_updated: {last_updated}, category_type: {category_type}")
            
            if index % 100 == 0:
                logger.info(f"Processed {index}/{total_schemes} schemes")
                time.sleep(2)  # Increased delay to avoid rate limits
                
        except RequestException as e:
            logger.error(f"RequestException processing scheme {scheme_code}: {str(e)}", exc_info=True)
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError processing scheme {scheme_code}: {str(e)}", exc_info=True)
        except ValueError as e:
            logger.error(f"ValueError processing scheme {scheme_code}: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"Error processing scheme {scheme_code}: {str(e)}", exc_info=True)
        
        # Add a small delay between each request
        time.sleep(0.1)
    
    return data

def update_mutual_fund_data():
    logger.info("Starting daily mutual fund NAV update")
    connection = create_database_connection()
    if connection:
        try:
            existing_scheme_codes = get_existing_scheme_codes(connection)
            mf = Mftool()
            fund_data = fetch_mutual_fund_nav_data(mf, existing_scheme_codes)
            
            cursor = connection.cursor()
            batch_size = 1000
            for i in range(0, len(fund_data), batch_size):
                batch = fund_data[i:i+batch_size]
                update_fund_nav(cursor, batch)
            
            connection.commit()
            logger.info(f"Updated NAV for {len(fund_data)} mutual funds in the database")
        except Error as e:
            logger.error(f"Database error: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                logger.info("MySQL connection is closed")
    else:
        logger.error("Failed to connect to the database")

def check_and_add_new_schemes():
    logger.info("Checking for new mutual fund schemes")
    connection = create_database_connection()
    if connection:
        try:
            existing_scheme_codes = set(get_existing_scheme_codes(connection))
            mf = Mftool()
            all_scheme_codes = set(mf.get_scheme_codes().keys())
            new_scheme_codes = all_scheme_codes - existing_scheme_codes
            
            if new_scheme_codes:
                new_fund_data = fetch_mutual_fund_full_data(mf, new_scheme_codes)
                cursor = connection.cursor()
                batch_size = 1000
                for i in range(0, len(new_fund_data), batch_size):
                    batch = new_fund_data[i:i+batch_size]
                    insert_or_update_fund(cursor, batch)
                connection.commit()
                logger.info(f"Added {len(new_fund_data)} new mutual fund schemes to the database")
            else:
                logger.info("No new mutual fund schemes found")
        except Error as e:
            logger.error(f"Database error: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                logger.info("MySQL connection is closed")
    else:
        logger.error("Failed to connect to the database")


def check_and_add_new_schemes(limit =  None):
    logger.info(f"Checking for new mutual fund schemes (limit: {limit if limit else 'None'})")
    connection = create_database_connection()
    if connection:
        try:
            existing_scheme_codes = set(get_existing_scheme_codes(connection))
            mf = Mftool()
            all_scheme_codes = set(mf.get_scheme_codes().keys())
            new_scheme_codes = list(all_scheme_codes - existing_scheme_codes)
            
            if limit:
                new_scheme_codes = new_scheme_codes[:limit]
            
            if new_scheme_codes:
                new_fund_data = fetch_mutual_fund_full_data(mf, new_scheme_codes)
                total_sucessful = 0
                total_failed = 0
                batch_size = 1000
                for i in range(0, len(new_fund_data), batch_size):
                    batch = new_fund_data[i:i+batch_size]
                    successful, failed = insert_or_update_fund(connection, batch)
                    total_sucessful += successful
                    total_failed +=  failed
                    logger.info(f"Batch{i//batch_size +1}:")
                    for fund in batch:
                        logger.info(f"{fund}")
                connection.commit()
                logger.info(f"Added {len(new_fund_data)} new mutual fund schemes to the database")
                logger.info(f"Total sucessful updates: {total_sucessful}")
                logger.info(f"Total failed_updates: {total_failed}")
            else:
                logger.info("No new mutual fund schemes found")
        except Error as e:
            logger.error(f"Error: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                logger.info("MySQL connection is closed")
    else:
        logger.error("Failed to connect to the database")

def schedule_daily_update():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    scheduled_time = now.replace(hour=18, minute=0, second=0, microsecond=0)  
    
    if now > scheduled_time:
        scheduled_time += timedelta(days=1)
    
    time_until_run = (scheduled_time - now).total_seconds()
    
    logger.info(f"Scheduled next daily update at {scheduled_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    time.sleep(time_until_run)
    update_mutual_fund_data()

def schedule_monthly_check():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    scheduled_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=28) # Approximate a month
    
    time_until_run = (scheduled_time - now).total_seconds()
    
    logger.info(f"Scheduled next monthly check at {scheduled_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    time.sleep(time_until_run)
    check_and_add_new_schemes()

if __name__ == "__main__":
    schedule.every().day.at("18:00").do(update_mutual_fund_data)
    schedule.every(4).weeks.do(check_and_add_new_schemes)

    logger.info("Running immediate test of daily update")
    update_mutual_fund_data()

    logger.info("Running immediate test of monthly check")
    check_and_add_new_schemes(limit = None)

    while True:
        schedule.run_pending()
        time.sleep(1)
