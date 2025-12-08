from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
import lakefs
import requests

def print_diff(diff):
    results = map(
        lambda n:[n.path,n.path_type,n.size_bytes,n.type],
        diff)

    from tabulate import tabulate
    print(tabulate(
        results,
        headers=['Path','Path Type','Size(Bytes)','Type']))

def print_commit(log):
    from datetime import datetime
    from pprint import pprint

    print('Message:', log.message)
    print('ID:', log.id)
    print('Committer:', log.committer)
    print('Creation Date:', datetime.utcfromtimestamp(log.creation_date).strftime('%Y-%m-%d %H:%M:%S'))
    print('Parents:', log.parents)
    print('Metadata:')
    pprint(log.metadata)

def lakefs_ui_endpoint(lakefsEndPoint):
    if lakefsEndPoint.startswith('http://host.docker.internal'):
        lakefsUIEndPoint = lakefsEndPoint.replace('host.docker.internal','127.0.0.1')
    elif lakefsEndPoint.startswith('http://lakefs'):
        lakefsUIEndPoint = lakefsEndPoint.replace('lakefs','127.0.0.1')
        if lakefs.client.Client().sdk_client.internal_api.get_lake_fs_version().version_context == 'lakeFS-Enterprise':
            lakefsUIEndPoint = lakefsUIEndPoint.replace('8000','8084')
    else:
        lakefsUIEndPoint = lakefsEndPoint
        
    return lakefsUIEndPoint

def lakefs_iceberg_glue_catalog_push(lakefsEndPoint, lakefsAccessKey, lakefsSecretKey,
                                     lakefs_repo_name, lakefs_reference,
                                     iceberg_namespace, iceberg_table_name, glue_database):
    # Push to Glue
    # Construct the push endpoint URL
    # Endpoint: POST /api/v1/iceberg/remotes/{catalog_id}/push
    glue_catalog_id = os.getenv("GLUE_CATALOG_ID", "glue_catalog")
    push_url = f"{lakefsEndPoint}/api/v1/iceberg/remotes/{glue_catalog_id}/push"
    
    payload = {
        "source": {
            "repository_id": lakefs_repo_name,
            "reference_id": lakefs_reference,
            "namespace": [iceberg_namespace],
            "table": iceberg_table_name
        },
        "destination": {
            "namespace": [glue_database],
            "table": iceberg_table_name
        },
        "create_namespace": True,
        "force_update": True
    }
    
    auth = (lakefsAccessKey, lakefsSecretKey)
    response = requests.post(push_url, json=payload, auth=auth)
    
    if response.status_code == 204:
        response_text = f"{iceberg_table_name} table was successfully pushed to Glue Database {glue_database}"
    else:
        response_text = response.text
    
    return response_text
