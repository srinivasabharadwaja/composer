#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example DAG using GCSToBigQueryOperator.
"""

import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
#rom airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago

PROJECT_ID='trainingproject-317506'
DATASET_NAME = 'airflow_test1'
TABLE_NAME = 'temp_table'
TABLE='temp1'
BUCKET_1='bharadwaja-gcp-training'
OBJECT_1='assignment.csv'
BUCKET_1_DST='bharadwaja-gcp-training'



dag = models.DAG(
    dag_id='assignment1',
    start_date=days_ago(2),
    schedule_interval=datetime.timedelta(days=1),
    tags=['example'],
)

# create_test_dataset = BigQueryCreateEmptyDatasetOperator(
#     task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
# )

# [START howto_operator_gcs_to_bigquery]

start = DummyOperator(
        task_id='start',
        dag=dag
    )
#invoice_and_item_number	date	store_number	store_name	address	city	zip_code	
#store_location	county_number	county	category	category_name	vendor_number	
# vendor_name	item_number	item_description	pack	bottle_volume_ml	state_bottle_cost	state_bottle_retail	bottles_sold	sale_dollars	volume_sold_liters	volume_sold_gallons

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='bharadwaja-gcp-training',
    source_objects=['assignment.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
 {"name": "invoice_and_item_number", "type": "STRING"},
  {"name": "date", "type": "STRING"},
 {"name": "store_number", "type": "STRING"},
 {"name": "store_name", "type": "STRING"},
 {"name": "address", "type": "STRING"},
 {"name": "city", "type": "STRING"},
 {"name": "zip_code", "type": "STRING"},
 {"name": "store_location", "type": "STRING"},
 {"name": "county_number", "type": "STRING"},
 #{"name": "emp_name", "type": "STRING"},

    
     ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

execute_query_save = BigQueryExecuteQueryOperator(
            task_id="execute_query_save",
            sql=f"SELECT * FROM {DATASET_NAME}.{TABLE_NAME} where store_number='2190'",
            use_legacy_sql=False,
            destination_dataset_table=f"{DATASET_NAME}.{TABLE}",
            write_disposition='WRITE_TRUNCATE',
            dag=dag,

        )

bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table=f"{DATASET_NAME}.{TABLE}",
        destination_cloud_storage_uris=["gs://bharadwaja-gcp-training/export-bigquery.csv"],
        dag=dag,
    )

copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=BUCKET_1,
        source_object=OBJECT_1,
        destination_bucket=BUCKET_1_DST,  # If not supplied the source_bucket value will be used
        destination_object="backup_" + OBJECT_1,  # If not supplied the source_object value will be used
    )

delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        dag=dag,

    )

end = DummyOperator(
        task_id='end'
    )

    
start >>load_csv >> execute_query_save >>[ delete_table,copy_single_file,bigquery_to_gcs] >> end