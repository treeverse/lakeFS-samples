from dagster import asset

import lakefs_client
from lakefs_client.client import LakeFSClient
import os

@asset
def create_lakefs_client():
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
