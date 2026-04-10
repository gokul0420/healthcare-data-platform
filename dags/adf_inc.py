import time
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ------------------------
# CONFIG
# ------------------------
DATABRICKS_HOST = "https://adb-7405612775128640.0.azuredatabricks.net"
TOKEN = "dapife110c952884cc92a0f1684a532bfb30"
CLUSTER_ID = "0329-094655-3ckd5mzv"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}
#adf configuration
ADF_RESOURCE_GROUP = "ajippangroup"
ADF_FACTORY_NAME = "capfac"
ADF_PIPELINE_NAME = "capstone_pipeline"

ADF_API_VERSION = "2018-06-01"

ADF_SUBSCRIPTION_ID = "ffc912b1-3437-46ba-91ab-b3909dd1de9e"
#get azure token
def get_adf_token():
    url = "https://login.microsoftonline.com/d132344b-c0a5-4920-bacd-83dd81428054/oauth2/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": "41abd884-644e-4175-a072-18b6d57e3b81",
        "client_secret": "dl88Q~TtK3VK1Sneszi6q7bbfb~1zqL6ANvXHaqk",
        "resource": "https://management.azure.com/"
    }

    response = requests.post(url, data=payload)
    return response.json()["access_token"]
#trigger adf
def run_adf_pipeline():
    token = get_adf_token()

    url = f"https://management.azure.com/subscriptions/{ADF_SUBSCRIPTION_ID}/resourceGroups/{ADF_RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/{ADF_FACTORY_NAME}/pipelines/{ADF_PIPELINE_NAME}/createRun?api-version={ADF_API_VERSION}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(url, headers=headers)

    print("ADF STATUS:", response.status_code)
    print("ADF RESPONSE:", response.text)

    if response.status_code != 200:
        raise Exception("ADF Pipeline Trigger Failed ❌")

    run_id = response.json()["runId"]

    # 🔥 WAIT FOR COMPLETION
    while True:
        time.sleep(15)

        status_url = f"https://management.azure.com/subscriptions/{ADF_SUBSCRIPTION_ID}/resourceGroups/{ADF_RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/{ADF_FACTORY_NAME}/pipelineruns/{run_id}?api-version={ADF_API_VERSION}"

        status_headers = {
            "Authorization": f"Bearer {token}"
        }

        status_resp = requests.get(status_url, headers=status_headers).json()

        status = status_resp["status"]

        print("ADF STATUS:", status)

        if status in ["Succeeded", "Failed"]:
            if status == "Succeeded":
                print("ADF completed successfully ✅")
                break
            else:
                raise Exception("ADF failed ❌")

# ------------------------
# FUNCTION TO RUN NOTEBOOK
# ------------------------
def run_notebook(notebook_path):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"

    payload = {
        "run_name": "airflow_run",
        "existing_cluster_id": CLUSTER_ID,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }

    response = requests.post(url, headers=HEADERS, json=payload)

    print("STATUS:", response.status_code)
    data = response.json()
    print("RESPONSE:", data)

    if response.status_code != 200:
        raise Exception(f"Databricks API failed: {response.text}")

    run_id = data.get("run_id")

    # 🔥 WAIT FOR COMPLETION
    while True:
        time.sleep(10)

        status_url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
        status_resp = requests.get(status_url, headers=HEADERS).json()

        state = status_resp["state"]["life_cycle_state"]
        result = status_resp["state"].get("result_state")

        print("STATE:", state, "RESULT:", result)

        if state == "TERMINATED":
            if result == "SUCCESS":
                print("Notebook completed successfully ✅")
                break
            else:
                raise Exception(f"Notebook failed ❌: {status_resp}")

# ------------------------
# DAG CONFIG
# ------------------------
default_args = {
    'owner': 'gokul',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='adf_inc',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    #adf
    adf_task = PythonOperator(
    task_id='run_adf_pipeline',
    python_callable=run_adf_pipeline
    )

    # ------------------------
    # BRONZE
    # ------------------------
    bronze = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=run_notebook,
        op_args=["/Users/gokulkrishna0420@gmail.com/cap_bronze"]
    )

    # ------------------------
    # SILVER
    # ------------------------
    silver = PythonOperator(
        task_id='silver_transformation',
        python_callable=run_notebook,
        op_args=["/Users/gokulkrishna0420@gmail.com/cap_silver"]
    )

    # ------------------------
    # VALIDATION
    # ------------------------
    validation = PythonOperator(
        task_id='validation',
        python_callable=run_notebook,
        op_args=["/Users/gokulkrishna0420@gmail.com/cap_valid"]
    )

    # ------------------------
    # GOLD
    # ------------------------
    gold = PythonOperator(
        task_id='gold_aggregation',
        python_callable=run_notebook,
        op_args=["/Users/gokulkrishna0420@gmail.com/cap_gold"]
    )

    # ------------------------
    # FLOW
    # ------------------------
    adf_task >> bronze >> silver >> validation >> gold