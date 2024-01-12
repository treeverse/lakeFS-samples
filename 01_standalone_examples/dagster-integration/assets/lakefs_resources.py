from dagster import resource
import os

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