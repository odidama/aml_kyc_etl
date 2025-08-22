import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
import yaml
import requests
import json
import pandas as pd
from pathlib import Path
from io import StringIO

load_dotenv()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logging.basicConfig(level=logging.INFO, filename=os.path.dirname(os.path.abspath('logs.txt')) + '/logs.txt',
                    format='%(asctime)s :: %(levelname)s :: %(message)s')


# def fetch_yaml_items():
#     yaml_file = f"{Path(__file__).parent.absolute()}/necessary_items.yaml"
#     with open(yaml_file) as file:
#         params = yaml.load(file, Loader=yaml.FullLoader)
#     return params


def connect_to_db():
    try:
        neon_conn_string = os.getenv("NEON_DATABASE_URL")
        engine = create_engine(neon_conn_string)
        return engine
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None


def api_call(endpoint):
    try:
        endpoint = endpoint
        base_url = os.getenv("ICIJ_BASE_URL")
        url = f"{base_url}{endpoint}/?renderer=oldb"
        payload = requests.get(url, timeout=5)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: {e}")
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"An unexpected error occurred during the request: {e}")
    except ValueError as e:
        print(f"JSON decoding error: {e}")
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
    else:
        return payload.json()


def normalize_payload_load_data():
    yaml_file = f"{Path(__file__).parent.absolute()}/necessary_items.yaml"
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)['api_endpoint_list']
        for e_point in data:
            if 'addresses' in e_point:
                rdbms_format = []
                normalized_data = []
                json_data = api_call(e_point)
                logging.info('API call successful...')
                for items in json_data['data']:
                    data_prep = {'Address_id': items['id'],
                                 'Schema': items['schema'],
                                 'Property_Name': items['properties']['name'],
                                 'Country_Code': str(items['properties']['country_codes']).strip('[').strip(']'),
                                 'Icij_id': items['properties']['icij_id'],
                                 'Data_Source': items['properties']['data_source'],
                                 'Note': items['properties']['note'],
                                 'Address': items['properties']['address'],
                                 'Valid_Until': items['properties']['valid_until']}
                normalized_data.append(data_prep)
                logging.info('Normalizing Addresses api data successful...')
                rdbms_format = pd.DataFrame(
                    pd.read_json(StringIO(json.dumps(normalized_data, sort_keys=True, indent=4))))
                try:
                    rdbms_format.to_sql(name='address', con=connect_to_db(), if_exists='replace', index=False)
                except Exception as e:
                    print(e)
                else:
                    print('Address data inserted successfully')
                    logging.info('Address data insert successful...')

            if 'officers' in e_point:
                rdbms_format = []
                normalized_data = []
                json_data = api_call(e_point)
                for items in json_data['data']:
                    data_prep = {'PersonID': items['id'],
                                 'Schema': items['schema'],
                                 'Name': items['properties']['name'],
                                 'Country_Code': str(items['properties']['country_codes']).strip('[').strip(']'),
                                 'Icij_Id': items['properties']['icij_id'],
                                 'Data_Source': items['properties']['data_source'],
                                 'Valid_Until': items['properties']['valid_until']}
                    normalized_data.append(data_prep)
                logging.info('Normalizing persons api data successful...')
                rdbms_format = pd.DataFrame(
                     pd.read_json(StringIO(json.dumps(normalized_data, sort_keys=True, indent=4))))
                try:
                    rdbms_format.to_sql(name='persons', con=connect_to_db(), if_exists='replace', index=False)
                except Exception as e:
                    print(e)
                else:
                    logging.info('Persons data insert successful...')
                    print('Persons data inserted successfully')

normalize_payload_load_data()



# def normalize_payload_write_data(*args, **kwargs):
#     api_endpoints = fetch_necessary_items()['api_endpoint_list']
#     for key, endpoint in api_endpoints.items():
#         normalized_data = []
#         rdbms_format = []
#         json_data = api_call(endpoint)
#         if key == 'address':
#             logging.info('Normalizing Addresses api data...')
#             for items in json_data['data']:
#                 data_prep = {'Address_id': items['id'],
#                              'Schema': items['schema'],
#                              'Property_Name': items['properties']['name'],
#                              'Country_Code': str(items['properties']['country_codes']).strip('[').strip(']'),
#                              'Icij_id': items['properties']['icij_id'],
#                              'Data_Source': items['properties']['data_source'],
#                              'Note': items['properties']['note'],
#                              'Address': items['properties']['address'],
#                              'Valid_Until': items['properties']['valid_until']}
#                 normalized_data.append(data_prep)
#                 logging.info('Normalizing Addresses api data successful...')
#                 rdbms_format = pd.DataFrame(
#                     pd.read_json(StringIO(json.dumps(normalized_data, sort_keys=True, indent=4))))
#                 try:
#                     rdbms_format.to_sql(key, con=connect_to_db(), if_exists='replace', index=False)
#                 except Exception as e:
#                     print(e)
#                 else:
#                     print('Data inserted successfully')
#         elif key == 'persons':
#             logging.info('Normalizing Persons api data...')
#             for items in json_data['data']:
#                 data_prep = {'PersonID': items['id'],
#                              'Schema': items['schema'],
#                              'Name': items['properties']['name'],
#                              'Country_Code': str(items['properties']['country_codes']).strip('[').strip(']'),
#                              'Icij_Id': items['properties']['icij_id'],
#                              'Data_Source': items['properties']['data_sources'],
#                              'Valid_Until': items['properties']['valid_until']}
#                 normalized_data.append(data_prep)
#                 logging.info('Normalizing Addresses api data successful...')
#                 rdbms_format = pd.DataFrame(
#                     pd.read_json(StringIO(json.dumps(normalized_data, sort_keys=True, indent=4))))
#                 try:
#                     rdbms_format.to_sql(key, con=connect_to_db(), if_exists='replace', index=False)
#                 except Exception as e:
#                     print(e)
#
#         return rdbms_format
