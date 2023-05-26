# [START tutorial]
# [START import_module]
import json
from dagster import job, op, get_dagster_logger, in_process_executor, mem_io_manager, Config
import io
from assets.lakefs_client_creation import create_lakefs_client
from assets.lakefs_ops_config import LakeFSOpConfig
# [END import_module]

# [START extract]
@op
def extract():
    """
    #### Extract task
    A simple Extract task to get data ready for the rest of the data
    pipeline. In this case, getting data is simulated by reading from a
    hardcoded JSON string.
    """
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

    order_data_dict = json.loads(data_string)
    return order_data_dict
# [END extract]

# [START transform]
@op
def transform(order_data_dict: dict):
    """
    #### Transform task
    A simple Transform task which takes in the collection of order data and
    computes the total order value.
    """
    total_order_value = 0

    for value in order_data_dict.values():
        total_order_value += value

    return total_order_value
# [END transform]

# [START load]
@op
def load(total_order_value: float, client, config: LakeFSOpConfig):
    """
    #### Load task
    A simple Load task which takes in the result of the Transform task and
    instead of saving it to end us  er review, just prints it out.
    """

    print(f"Total order value is: {total_order_value:.2f}")

    # [START of lakeFS Code]
    contentToUpload = io.BytesIO(f"Total order value is: {total_order_value:.2f}".encode('utf-8'))
    client.objects.upload_object(
        repository=config.repo,
        branch=config.newBranch,
        path="total_order_value.txt", content=contentToUpload)
    # [END of lakeFS Code]
# [END load]

@job(resource_defs={"io_manager": mem_io_manager}, executor_def=in_process_executor)
def lakefs_tutorial_taskflow_api_etl():
    client = create_lakefs_client()
    # [START main_flow]
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary, client)
    # [END main_flow]

# [END tutorial]
