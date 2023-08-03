from prefect import flow, task, runtime, variables, get_run_logger
from prefect_lakefs.credentials import LakeFSCredentials
from prefect_lakefs.tasks import delete_object, diff_branch, commit, create_branch
from prefect_lakefs.refs import merge_into_branch
from lakefs_tutorial_taskflow_api_etl import lakefs_tutorial_taskflow_api_etl
from lakefs_client import models
from lakefs_demo import generate_flow_run_name

def print_commit_result(result, message, lakefs_ui_endpoint, repo):
    my_logger = get_run_logger()
    my_logger.info('lakeFS URL is: ' + lakefs_ui_endpoint \
        + '/repositories/' + repo + '/commits/' + result \
        + ' and ' + message + result)

def print_branch_creation_result(result, message, lakefs_ui_endpoint, repo):
    my_logger = get_run_logger()
    my_logger.info('lakeFS URL is: ' + lakefs_ui_endpoint \
        + '/repositories/' + repo + '/objects?ref=' + result \
        + ' and ' + message + result)

@flow(name='Diff to check uncommitted changes', flow_run_name=generate_flow_run_name)
def diff_source_branch(*args):
    future = diff_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get('repo'),
        branch=variables.get('source_branch'),
    )
    return future

@flow(name='Commit any uncommitted changes', flow_run_name=generate_flow_run_name)
def commit_source_branch(diff_source_branch_result):
    if diff_source_branch_result.pagination.results > 0:
        future = commit.submit(
            lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
            repository=variables.get('repo'),
            branch=variables.get('source_branch'),
            message="Deleted existing demo objects using Prefect!",
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    },
        )
        print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                            lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Delete Demo Objects', flow_run_name=generate_flow_run_name)
def delete_demo_objects():
    future1 = delete_object.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get('repo'),
        branch=variables.get('source_branch'),
        path="total_order_value.txt"
    )
    future2 = diff_source_branch(future1)
    commit_source_branch(future2)
    
@flow(name='Create Branch for ETL', flow_run_name=generate_flow_run_name)
def create_etl_branch(new_branch: str, *args):
    future = create_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get('repo'),
        name=new_branch,
        source=variables.get('source_branch'),
    )

    print_branch_creation_result(result=new_branch, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Commit ETL Branch', flow_run_name=generate_flow_run_name)
def commit_etl_branch(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get('repo'),
        branch=new_branch,
        message='committing to lakeFS using Prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Merge ETL Branch', flow_run_name=generate_flow_run_name)
def merge_etl_branch(new_branch: str, *args):
    future = merge_into_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get('repo'),
        source_ref=new_branch, 
        destination_branch=variables.get('source_branch'),
        merge=models.Merge(
            message='merging ' + new_branch + ' to the ' \
                + variables.get('source_branch') + ' branch',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
        )
    )
    print_commit_result(result=future.result().reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))
    
@flow(name='lakeFS Wrapper DAG', flow_run_name=generate_flow_run_name)
def lakefs_wrapper_dag():
    new_branch = variables.get('new_branch') + runtime.flow_run.scheduled_start_time.strftime("_%Y%m%dT%H%M%S")
    delete_demo_objects_return = delete_demo_objects()
    create_etl_branch_return = create_etl_branch(new_branch, delete_demo_objects_return)
    lakefs_tutorial_taskflow_api_etl_return = lakefs_tutorial_taskflow_api_etl(new_branch, create_etl_branch_return)
    commit_etl_branch_return = commit_etl_branch(new_branch, lakefs_tutorial_taskflow_api_etl_return)
    merge_etl_branch(new_branch, commit_etl_branch_return)
    return runtime.flow_run.id
