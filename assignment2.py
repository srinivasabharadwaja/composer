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
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
#rom airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago

PROJECT_ID='trainingproject-317506'
DATASET_NAME = 'airflow_test1'

BUCKET_1='bharadwaja-gcp-training-2'

BUCKET_1_DST='bharadwaja-gcp-training'
#BIGQUERY_TO_GCS='gs://gcp-training-saisreeram/export-bigquery.csv'
#dag_config = Variable.get("example_variables_config", deserialize_json=True)
#dag_config=dag_config["{{ dag_run.conf['name'] }}"]
#TABLE_NAME = dag_config["{{ dag_run.conf['name'] }}"]["TABLE_NAME"]
# TABLE=dag_config["{{ dag_run.conf['name'] }}"]['TABLE']
# SCHEMA=dag_config["{{ dag_run.conf['name'] }}"]['SCHEMA']
# QUERY=dag_config["{{ dag_run.conf['name'] }}"]['QUERY']
# BIGQUERY_TO_GCS=dag_config["{{ dag_run.conf['name'] }}"]['BIGQUERY_TO_GCS']
# ARCHIVE=dag_config["{{ dag_run.conf['name'] }}"]['ARCHIVE']

#OBJECT_1='assignment.csv'

dag = models.DAG(
    dag_id='assignment2',
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example'],
)

# [START howto_operator_gcs_to_bigquery]

start = DummyOperator(
        task_id='start',
        dag=dag
    )

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='bharadwaja-gcp-training-2',
    source_objects=["{{ dag_run.conf['name'] }}"],
    destination_project_dataset_table=DATASET_NAME+"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE_NAME"] }}',
    #schema_fields='{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["SCHEMA"] }}',
    autodetect = True,

    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    dag=dag,
)

execute_query_save = BigQueryExecuteQueryOperator(
            task_id="execute_query_save",
            sql=str('{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["QUERY"] }}'),
            use_legacy_sql=False,
            destination_dataset_table=DATASET_NAME+"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE"] }}',
            write_disposition='WRITE_TRUNCATE',
            dag=dag,

        )

bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table=DATASET_NAME+"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE"] }}',
        destination_cloud_storage_uris=['{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["BIGQUERY_TO_GCS"] }}'],
        dag=dag,
    )

copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=BUCKET_1,
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=BUCKET_1_DST,  # If not supplied the source_bucket value will be used
        destination_object='{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["ARCHIVE"] }}' + "{{ dag_run.conf['name'] }}",  # If not supplied the source_object value will be used
    )

delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=PROJECT_ID +"." +DATASET_NAME+"."+'{{ var.json.get("example_variables_config")[dag_run.conf["name"]]["TABLE_NAME"] }}',
        dag=dag,

    )

end = DummyOperator(
        task_id='end'
    )

    
start >> load_csv >> execute_query_save >>[ delete_table,copy_single_file,bigquery_to_gcs] >> end



#curl -v https://ye83b79b6ccb42244p-tp.appspot.com 2>&1 >/dev/null | grep "location:"

#4816032679-m47u8q57nv800679c2cndgbvlsspp2e4.apps.googleusercontent.com