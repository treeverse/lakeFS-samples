from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
import requests
import lakefs_enterprise_sdk
from lakefs_enterprise_sdk import (
    DatasetCreation,
    DatasetDataItem,
    DatasetDefinition,
    DatasetLocator,
    DatasetPublish,
    DatasetRef,
    DatasetUpdate,
)
from lakefs_enterprise_sdk.exceptions import ApiException

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

def datasets_build_definition(DATASET_NAME, DATASET_DESCRIPTION, PUBLISH_MESSAGE, REPO, COMMIT, SOURCE_PATHS, TARGET_FOLDER) -> DatasetCreation:
    """Turn the path list into object-type data items pinned to COMMIT."""
    data_items = [
        DatasetDataItem(
            target=f"{TARGET_FOLDER}/{path.split('/')[-1]}",  # unique read address
            repository=REPO,
            ref=DatasetRef(type="commit", id=COMMIT),
            type="object",
            object=DatasetLocator(path=path),
        )
        for path in SOURCE_PATHS
    ]
    return DatasetCreation(
        name=DATASET_NAME,
        definition=DatasetDefinition(
            description=DATASET_DESCRIPTION,
            data=data_items,
        ),
        publish=DatasetPublish(message=PUBLISH_MESSAGE),
    )


def datasets_create_dataset(CONFIGURATION, DATASET_NAME, DATASET_DESCRIPTION, PUBLISH_MESSAGE, REPO, COMMIT, SOURCE_PATHS, TARGET_FOLDER) -> None:
    dataset_creation = datasets_build_definition(DATASET_NAME, DATASET_DESCRIPTION, PUBLISH_MESSAGE, REPO, COMMIT, SOURCE_PATHS, TARGET_FOLDER)

    with lakefs_enterprise_sdk.ApiClient(CONFIGURATION) as api_client:
        api_instance = lakefs_enterprise_sdk.DatasetsApi(api_client)
        try:
            dataset = api_instance.create_dataset(dataset_creation)
        except ApiException as e:
            # Dataset names are globally unique and immutable, so re-running the
            # notebook publishes a new version of the existing dataset instead of
            # failing on the name conflict.
            if e.status != 409:
                raise RuntimeError(f"Create failed [{e.status}]: {e.body}") from e
            try:
                dataset = api_instance.update_dataset(
                    DATASET_NAME,
                    DatasetUpdate(
                        definition=dataset_creation.definition,
                        publish=DatasetPublish(message=PUBLISH_MESSAGE),
                    ),
                )
            except ApiException as e:
                # Re-running with an unchanged definition has nothing to publish.
                if e.status == 400 and "no changes" in str(e.body):
                    dataset = api_instance.get_dataset(DATASET_NAME)
                    print(f"Dataset '{DATASET_NAME}' is already up to date -> v{dataset.version or 1}")
                    return
                raise RuntimeError(f"Update failed [{e.status}]: {e.body}") from e

    version = f"v{dataset.version or 1}"
    print(f"Published dataset '{DATASET_NAME}' -> {version}")
