from dagster import resource

import lakefs_client
from lakefs_client.client import LakeFSClient
import os

@resource
def lakefs_client_resource():
    lakefsEndPoint = os.getenv("LAKEFS_ENDPOINT")
    lakefsAccessKey = os.getenv("LAKEFS_CREDENTIALS_ACCESS_KEY_ID")
    lakefsSecretKey = os.getenv("LAKEFS_CREDENTIALS_SECRET_ACCESS_KEY")
    # lakeFS credentials and endpoint
    configuration = lakefs_client.Configuration()
    configuration.username = lakefsAccessKey
    configuration.password = lakefsSecretKey
    configuration.host = lakefsEndPoint

    client = LakeFSClient(configuration)
    return client    

@resource
def lakefs_ui_endpoint():
    lakefsEndPoint = os.getenv("LAKEFS_ENDPOINT")
    if lakefsEndPoint.startswith('http://host.docker.internal'):
        lakefsUIEndPoint = 'http://127.0.0.1:8000'
    elif lakefsEndPoint.startswith('http://lakefs'):
        lakefsUIEndPoint = 'http://127.0.0.1:28000'
    else:
        lakefsUIEndPoint = lakefsEndPoint
        
    return lakefsUIEndPoint

@resource
def dagster_ui_endpoint():
    dagsterUIEndPoint = 'http://127.0.0.1:3000'
        
    return dagsterUIEndPoint