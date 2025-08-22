from prefect import task, flow
from utilities.helpers import connect_to_db, api_call, normalize_payload_load_data


@task()
def call_endpoints():
    return normalize_payload_load_data()


@flow
def etl_workflow():
    run_flow = call_endpoints()


if __name__ == '__main__':
    etl_workflow()