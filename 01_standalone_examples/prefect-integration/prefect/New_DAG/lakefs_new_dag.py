from prefect import flow, runtime, variables, get_run_logger
from prefect_lakefs.credentials import LakeFSCredentials
from prefect_lakefs.tasks import delete_objects, upload_object, get_object, diff_branch, commit, create_branch
from prefect_lakefs.refs import merge_into_branch
import os
from io import StringIO
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

class NamedStringIO(StringIO):
    def __init__(self, content: str, name: str) -> None:
        super().__init__(content)
        self.name = name

@flow(name='Diff to check uncommitted changes', flow_run_name=generate_flow_run_name)
def diff_source_branch(*args):
    future = diff_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=variables.get("source_branch"))
    return future
    
@flow(name='Commit any uncommitted changes', flow_run_name=generate_flow_run_name)
def commit_source_branch(diff_source_branch_result):
    if diff_source_branch_result.pagination.results > 0:
        future = commit.submit(
            lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
            repository=variables.get("repo"),
            branch=variables.get("source_branch"),
            message='Deleted existing demo objects using prefect!',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
        )
        print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                            lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Delete Demo Objects', flow_run_name=generate_flow_run_name)
def delete_demo_objects():
    paths=[
        variables.get("file_name"),
        variables.get("new_path")+'_c0/'+variables.get("file_name"),
        variables.get("new_path")+'_c1/'+variables.get("file_name"),
        variables.get("new_path")+'_c2/'+variables.get("file_name"),
        variables.get("new_path")+'_c3/'+variables.get("file_name"),
        variables.get("new_path")+'_c4/'+variables.get("file_name"),
    ]
    
    future1 = delete_objects.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=variables.get("source_branch"),
        path_list=paths)
    future2 = diff_source_branch(future1)
    commit_source_branch(future2)    
    
@flow(name='Create Branch for ETL', flow_run_name=generate_flow_run_name)
def create_etl_branch(new_branch: str, *args):
    branch_name = new_branch
    future = create_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        name=branch_name,
        source=variables.get("source_branch"))

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Create Branch for Data Load', flow_run_name=generate_flow_run_name)
def create_etl_load_branch(new_branch: str, *args):
    # Create the branch to run on
    branch_name = new_branch + '_etl_load'
    future = create_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        name=branch_name,
        source=new_branch) 

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Create Branch for ETL Task1', flow_run_name=generate_flow_run_name)
def create_etl_task1_branch(new_branch: str, *args):
    # Create the branch to run on
    branch_name = new_branch + '_etl_task1'
    future = create_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        name=branch_name,
        source=new_branch + '_etl_load')

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Create Branch for ETL Task2', flow_run_name=generate_flow_run_name)
def create_etl_task2_branch(new_branch: str, *args):
    # Create the branch to run on
    branch_name = new_branch + '_etl_task2'
    future = create_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        name=branch_name,
        source=new_branch + '_etl_load')

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Create Branch for ETL Task3', flow_run_name=generate_flow_run_name)
def create_etl_task3_branch(new_branch: str, *args):
    # Create the branch to run on
    branch_name = new_branch + '_etl_task3'
    future = create_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        name=branch_name,
        source=new_branch + '_etl_load')

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Load Data File', flow_run_name=generate_flow_run_name)
def load_file(new_branch: str, *args):
    contentToUpload = open(variables.get("file_path")+'/'+variables.get("file_name"), 'rb')
    branch_name = new_branch + '_etl_load'
    future = upload_object.submit(
            lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
            repository=variables.get("repo"),
            branch=branch_name,
            path=variables.get("file_name"),
            content=contentToUpload)

    my_logger = get_run_logger()
    my_logger.info(
        'lakeFS URL for the data file is: ' + variables.get('lakefs_ui_endpoint') \
        + '/api/v1/repositories/' + variables.get("repo") + '/refs/' \
        + branch_name \
        + '/objects?path=' + variables.get("file_name"))

@flow(name='Commit Data File', flow_run_name=generate_flow_run_name)
def commit_load(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=new_branch + '_etl_load',
        message='committing etl_load to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Commit Changes for ETL Task1', flow_run_name=generate_flow_run_name)
def commit_task1(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=new_branch + '_etl_task1',
        message='committing etl_task1 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Commit Changes for ETL Task2_1', flow_run_name=generate_flow_run_name)
def commit_task2_1(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=new_branch + '_etl_task2',
        message='committing etl_task2_1 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Commit Changes for ETL Task2_2', flow_run_name=generate_flow_run_name)
def commit_task2_2(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=new_branch + '_etl_task2',
        message='committing etl_task2_2 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Commit Changes for ETL Task2_3', flow_run_name=generate_flow_run_name)
def commit_task2_3(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=new_branch + '_etl_task2',
        message='committing etl_task2_3 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Commit Changes for ETL Task3', flow_run_name=generate_flow_run_name)
def commit_task3(new_branch: str, *args):
    future = commit.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        branch=new_branch + '_etl_task3',
        message='committing etl_task3 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                + runtime.flow_run.id,
                }
    )
    print_commit_result(result=future.result().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@flow(name='Merge ETL Branch to Main Branch', flow_run_name=generate_flow_run_name)
def merge_etl_branch(new_branch: str, *args):
    source_branch_name = new_branch
    destination_branch_name = variables.get("source_branch")
    future = merge_into_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        source_ref=source_branch_name,
        destination_branch=destination_branch_name,
        merge=models.Merge(
            message='merging ' + source_branch_name + ' to the ' \
                + destination_branch_name + ' branch',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
        )
    )
    print_commit_result(result=future.result().reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Merge ETL Task1 Branch to Data Load Branch', flow_run_name=generate_flow_run_name)
def merge_etl_task1_branch(new_branch: str, *args):
    source_branch_name = new_branch + '_etl_task1'
    destination_branch_name = new_branch + '_etl_load'
    future = merge_into_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        source_ref=source_branch_name,
        destination_branch=destination_branch_name,
        merge=models.Merge(
            message='merging ' + source_branch_name + ' to the ' \
                + destination_branch_name + ' branch',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
            )
        )
    print_commit_result(result=future.result().reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Merge ETL Task2 Branch to Data Load Branch', flow_run_name=generate_flow_run_name)
def merge_etl_task2_branch(new_branch: str, *args):
    source_branch_name = new_branch + '_etl_task2'
    destination_branch_name = new_branch + '_etl_load'
    future = merge_into_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        source_ref=source_branch_name,
        destination_branch=destination_branch_name,
        merge=models.Merge(
            message='merging ' + source_branch_name + ' to the ' \
                + destination_branch_name + ' branch',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
        )
    )
    print_commit_result(result=future.result().reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Merge ETL Task3 Branch to Data Load Branch', flow_run_name=generate_flow_run_name)
def merge_etl_task3_branch(new_branch: str, *args):
    source_branch_name = new_branch + '_etl_task3'
    destination_branch_name = new_branch + '_etl_load'
    future = merge_into_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        source_ref=source_branch_name,
        destination_branch=destination_branch_name,
        merge=models.Merge(
            message='merging ' + source_branch_name + ' to the ' \
                + destination_branch_name + ' branch',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
        )
    )
    print_commit_result(result=future.result().reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='Merge Data Load Branch to ETL Branch', flow_run_name=generate_flow_run_name)
def merge_etl_load_branch(new_branch: str, *args):
    source_branch_name = new_branch + '_etl_load'
    destination_branch_name = new_branch
    future = merge_into_branch.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        source_ref=source_branch_name,
        destination_branch=destination_branch_name,
        merge=models.Merge(
            message='merging ' + source_branch_name + ' to the ' \
                + destination_branch_name + ' branch',
            metadata={"committed_from": "prefect-operator",
                    '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                    + runtime.flow_run.id,
                    }
        )
    )
    print_commit_result(result=future.result().reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@flow(name='ETL Task1', flow_run_name=generate_flow_run_name)
def etl_task1(new_branch: str, *args):
    branch_name = new_branch + '_etl_task1'
    future = get_object.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        ref=branch_name,
        path=variables.get("file_name"))
    content = str(future.result().read(), 'UTF-8')
    if len(content.split(",")) < 1:
        raise Exception("Column _c0 not found in schema")
    else:
        upload_object.submit(
                lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
                repository=variables.get("repo"),
                branch=branch_name,
                path=variables.get("new_path") + '_c0/' + variables.get("file_name"),
                content=NamedStringIO(content=content, name='content'))
        
@flow(name='ETL Task2_1', flow_run_name=generate_flow_run_name)
def etl_task2_1(new_branch: str, *args):
    branch_name = new_branch + '_etl_task2'
    future = get_object.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        ref=branch_name,
        path=variables.get("file_name"))
    content = str(future.result().read(), 'UTF-8')
    if len(content.split(",")) < 2:
        raise Exception("Column _c1 not found in schema struct<_c0:string>")
    else:
        upload_object.submit(
                lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
                repository=variables.get("repo"),
                branch=branch_name,
                path=variables.get("new_path") + '_c1/' + variables.get("file_name"),
                content=NamedStringIO(content=content, name='content'))
        
@flow(name='ETL Task2_2', flow_run_name=generate_flow_run_name)
def etl_task2_2(new_branch: str, *args):
    branch_name = new_branch + '_etl_task2'
    future = get_object.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        ref=branch_name,
        path=variables.get("file_name"))
    content = str(future.result().read(), 'UTF-8')
    if len(content.split(",")) < 3:
        raise Exception("Column _c2 not found in schema struct<_c0:string,_c1:string>")
    else:
        upload_object.submit(
                lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
                repository=variables.get("repo"),
                branch=branch_name,
                path=variables.get("new_path") + '_c2/' + variables.get("file_name"),
                content=NamedStringIO(content=content, name='content'))
        
@flow(name='ETL Task2_3', flow_run_name=generate_flow_run_name)
def etl_task2_3(new_branch: str, *args):
    branch_name = new_branch + '_etl_task2'
    future = get_object.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        ref=branch_name,
        path=variables.get("file_name"))
    content = str(future.result().read(), 'UTF-8')
    if len(content.split(",")) < 4:
        raise Exception("Column _c3 not found in schema struct<_c0:string,_c1:string,_c2:string>")
    else:
        upload_object.submit(
                lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
                repository=variables.get("repo"),
                branch=branch_name,
                path=variables.get("new_path") + '_c3/' + variables.get("file_name"),
                content=NamedStringIO(content=content, name='content'))
        
@flow(name='ETL Task3', flow_run_name=generate_flow_run_name)
def etl_task3(new_branch: str, *args):
    branch_name = new_branch + '_etl_task3'
    future = get_object.submit(
        lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
        repository=variables.get("repo"),
        ref=branch_name,
        path=variables.get("file_name"))
    content = str(future.result().read(), 'UTF-8')
    if len(content.split(",")) < 5:
        raise Exception("Column _c4 not found in schema struct<_c0:string,_c1:string,_c2:string,_c3:string>")
    else:
        upload_object.submit(
                lakefs_credentials=LakeFSCredentials.load(variables.get('lakefs_credential_name')),
                repository=variables.get("repo"),
                branch=branch_name,
                path=variables.get("new_path") + '_c4/' + variables.get("file_name"),
                content=NamedStringIO(content=content, name='content'))
        
@flow(name='lakeFS New DAG', flow_run_name=generate_flow_run_name)
def lakefs_new_dag():
    try:
        new_branch = variables.get('new_branch') + runtime.flow_run.scheduled_start_time.strftime("_%Y%m%dT%H%M%S")
        delete_demo_objects_return = delete_demo_objects()
        create_etl_branch_return = create_etl_branch(new_branch, delete_demo_objects_return)
        create_etl_load_branch_return = create_etl_load_branch(new_branch, create_etl_branch_return)
        load_file_return = load_file(new_branch, create_etl_load_branch_return)
        commit_load_return = commit_load(new_branch, load_file_return)
        create_etl_task1_branch_return = create_etl_task1_branch(new_branch, commit_load_return)
        create_etl_task2_branch_return = create_etl_task2_branch(new_branch, commit_load_return)
        create_etl_task3_branch_return = create_etl_task3_branch(new_branch, commit_load_return)
        etl_task1_return = etl_task1(new_branch, create_etl_task1_branch_return)
        commit_task1_return = commit_task1(new_branch, etl_task1_return)
        merge_etl_task1_branch_return = merge_etl_task1_branch(new_branch, commit_task1_return)
        etl_task2_1_return = etl_task2_1(new_branch, create_etl_task2_branch_return)
        commit_task2_1_return = commit_task2_1(new_branch, etl_task2_1_return)
        etl_task2_2_return = etl_task2_2(new_branch, commit_task2_1_return)
        commit_task2_2_return = commit_task2_2(new_branch, etl_task2_2_return)
        etl_task2_3_return = etl_task2_3(new_branch, commit_task2_2_return)
        commit_task2_3_return = commit_task2_3(new_branch, etl_task2_3_return)
        merge_etl_task2_branch_return = merge_etl_task2_branch(new_branch, commit_task2_3_return)
        etl_task3_return = etl_task3(new_branch, create_etl_task3_branch_return)
        commit_task3_return = commit_task3(new_branch, etl_task3_return)
        merge_etl_task3_branch_return = merge_etl_task3_branch(new_branch, commit_task3_return)
        merge_etl_load_branch_return = merge_etl_load_branch(new_branch,
                                                             merge_etl_task1_branch_return,
                                                             merge_etl_task2_branch_return,
                                                             merge_etl_task3_branch_return)
        merge_etl_branch(new_branch, merge_etl_load_branch_return)
        return runtime.flow_run.id
    except:
        return runtime.flow_run.id