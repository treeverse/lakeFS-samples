from prefect import flow, task, runtime, variables, get_run_logger
import lakefs
from lakefs_tutorial_taskflow_api_etl import lakefs_tutorial_taskflow_api_etl
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

@task(name='Delete Demo Objects')
def delete_demo_objects():
    branch = lakefs.repository(variables.get('repo')).branch(variables.get('source_branch'))
    branch.object("total_order_value.txt").delete()
    
    for diff in branch.uncommitted():    
        ref = branch.commit(
                message='Deleted existing demo objects using Prefect!',
                metadata={"committed_from": "prefect-operator",
                          '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                          + runtime.flow_run.id,
                         })

        print_commit_result(result=ref.get_commit().id, message='lakeFS commit id is: ',
                            lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Create Branch for ETL')
def create_etl_branch(new_branch: str):
    repo=lakefs.repository(variables.get('repo'))
    branch = repo.branch(new_branch).create(source_reference=variables.get('source_branch'))
    
    print_branch_creation_result(result=new_branch, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Commit ETL Branch')
def commit_etl_branch(new_branch: str):
    branch = lakefs.repository(variables.get('repo')).branch(new_branch)
    ref = branch.commit(
        message='committing to lakeFS using Prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                })
    print_commit_result(result=ref.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Merge ETL Branch')
def merge_etl_branch(new_branch: str):
    repo = lakefs.repository(variables.get('repo'))
    sourceBranch = repo.branch(new_branch)
    destinationBranch = repo.branch(variables.get('source_branch'))
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + new_branch + ' to the ' \
                + variables.get('source_branch') + ' branch',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))
    
@flow(name='lakeFS Wrapper DAG', flow_run_name=generate_flow_run_name)
def lakefs_wrapper_dag():
    new_branch = variables.get('new_branch') + runtime.flow_run.scheduled_start_time.strftime("_%Y%m%dT%H%M%S")
    delete_demo_objects_future = delete_demo_objects.submit()
    create_etl_branch_future = create_etl_branch.submit(new_branch, wait_for=[delete_demo_objects_future])
    lakefs_tutorial_taskflow_api_etl_future = lakefs_tutorial_taskflow_api_etl(new_branch, create_etl_branch_future)
    commit_etl_branch_future = commit_etl_branch.submit(new_branch, wait_for=[lakefs_tutorial_taskflow_api_etl_future])
    merge_etl_branch.submit(new_branch, wait_for=[commit_etl_branch_future])
    return runtime.flow_run.id
