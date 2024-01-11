from dagster import resource

import lakefs
from lakefs.client import Client
import os

@resource
def lakefs_client_resource():
    lakefsEndPoint = os.getenv("LAKECTL_SERVER_ENDPOINT_URL")
    lakefsAccessKey = os.getenv("LAKECTL_CREDENTIALS_ACCESS_KEY_ID")
    lakefsSecretKey = os.getenv("LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY")

    client = Client(
        host=lakefsEndPoint,
        username=lakefsAccessKey,
        password=lakefsSecretKey,
    )
    return client    

@resource
def lakefs_ui_endpoint():
    lakefsEndPoint = os.getenv("LAKECTL_SERVER_ENDPOINT_URL")
    if lakefsEndPoint.startswith('http://host.docker.internal'):
        lakefsUIEndPoint = 'http://127.0.0.1:8000'
    elif lakefsEndPoint.startswith('http://lakefs'):
        lakefsUIEndPoint = 'http://127.0.0.1:28000'
    else:
        lakefsUIEndPoint = lakefsEndPoint
        
    return lakefsUIEndPoint

@resource
def dagster_ui_endpoint():
    dagsterUIEndPoint = 'http://127.0.0.1:23000'
        
    return dagsterUIEndPoint