from prefect import flow, task, runtime, variables, get_run_logger
import lakefs
import os
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
    branch = lakefs.repository(variables.get("repo")).branch(variables.get("source_branch"))
    branch.object(variables.get("file_name")).delete()
    branch.object(variables.get("new_path")+'_c0/'+variables.get("file_name")).delete()
    branch.object(variables.get("new_path")+'_c1/'+variables.get("file_name")).delete()
    branch.object(variables.get("new_path")+'_c2/'+variables.get("file_name")).delete()
    branch.object(variables.get("new_path")+'_c3/'+variables.get("file_name")).delete()
    branch.object(variables.get("new_path")+'_c4/'+variables.get("file_name")).delete()
    
    for diff in branch.uncommitted():
        ref = branch.commit(
                message='Deleted existing demo objects using prefect!',
                metadata={"committed_from": "prefect-operator",
                          '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                          + runtime.flow_run.id,
                         })
        print_commit_result(result=ref.get_commit().id, message='lakeFS commit id is: ',
                            lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))
        break

@task(name='Create Branch for ETL')
def create_etl_branch(new_branch: str):
    branch_name = new_branch
    repo=lakefs.repository(variables.get("repo"))
    branch = repo.branch(branch_name).create(source_reference=variables.get("source_branch"))
    
    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Create Branch for Data Load')
def create_etl_load_branch(new_branch: str):
    branch_name = new_branch + '_etl_load'
    repo=lakefs.repository(variables.get("repo"))
    branch = repo.branch(branch_name).create(source_reference=new_branch)

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Create Branch for ETL Task1')
def create_etl_task1_branch(new_branch: str):
    branch_name = new_branch + '_etl_task1'
    repo=lakefs.repository(variables.get("repo"))
    branch = repo.branch(branch_name).create(source_reference=new_branch + '_etl_load')

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Create Branch for ETL Task2')
def create_etl_task2_branch(new_branch: str):
    branch_name = new_branch + '_etl_task2'
    repo=lakefs.repository(variables.get("repo"))
    branch = repo.branch(branch_name).create(source_reference=new_branch + '_etl_load')

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Create Branch for ETL Task3')
def create_etl_task3_branch(new_branch: str):
    branch_name = new_branch + '_etl_task3'
    repo=lakefs.repository(variables.get("repo"))
    branch = repo.branch(branch_name).create(source_reference=new_branch + '_etl_load')

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Load Data File')
def load_file(new_branch: str):
    branch_name = new_branch + '_etl_load'
    branch = lakefs.repository(variables.get("repo")).branch(branch_name)
    obj = branch.object(path=variables.get("file_name"))
    with open(variables.get("file_path")+'/'+variables.get("file_name"), mode='rb') as reader, obj.writer(mode='wb') as writer:
        writer.write(reader.read())

    my_logger = get_run_logger()
    my_logger.info(
        'lakeFS URL for the data file is: ' + variables.get('lakefs_ui_endpoint') \
        + '/api/v1/repositories/' + variables.get("repo") + '/refs/' \
        + branch_name \
        + '/objects?path=' + variables.get("file_name"))

@task(name='Commit Data File')
def commit_load(new_branch: str):
    branch = lakefs.repository(variables.get("repo")).branch(new_branch + '_etl_load')
    commit = branch.commit(
        message='committing etl_load to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=commit.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Commit Changes for ETL Task1')
def commit_task1(new_branch: str):
    branch = lakefs.repository(variables.get("repo")).branch(new_branch + '_etl_task1')
    commit = branch.commit(
        message='committing etl_task1 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=commit.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Commit Changes for ETL Task2_1')
def commit_task2_1(new_branch: str):
    branch = lakefs.repository(variables.get("repo")).branch(new_branch + '_etl_task2')
    commit = branch.commit(
        message='committing etl_task2_1 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=commit.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Commit Changes for ETL Task2_2')
def commit_task2_2(new_branch: str):
    branch = lakefs.repository(variables.get("repo")).branch(new_branch + '_etl_task2')
    commit = branch.commit(
        message='committing etl_task2_2 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=commit.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Commit Changes for ETL Task2_3')
def commit_task2_3(new_branch: str):
    branch = lakefs.repository(variables.get("repo")).branch(new_branch + '_etl_task2')
    commit = branch.commit(
        message='committing etl_task2_3 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=commit.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Commit Changes for ETL Task3')
def commit_task3(new_branch: str):
    branch = lakefs.repository(variables.get("repo")).branch(new_branch + '_etl_task3')
    commit = branch.commit(
        message='committing etl_task3 to lakeFS using prefect!',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=commit.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get('repo'))

@task(name='Merge ETL Branch to Main Branch')
def merge_etl_branch(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    sourceBranch = repo.branch(new_branch)
    destinationBranch = repo.branch(variables.get("source_branch"))
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + new_branch + ' to the ' \
                + variables.get("source_branch") + ' branch',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Merge ETL Task1 Branch to Data Load Branch')
def merge_etl_task1_branch(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    sourceBranch = repo.branch(new_branch + '_etl_task1')
    destinationBranch = repo.branch(new_branch + '_etl_load')
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + new_branch + '_etl_task1' + ' to the ' \
                + new_branch + '_etl_load' + ' branch',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Merge ETL Task2 Branch to Data Load Branch')
def merge_etl_task2_branch(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    sourceBranch = repo.branch(new_branch + '_etl_task2')
    destinationBranch = repo.branch(new_branch + '_etl_load')
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + new_branch + '_etl_task2' + ' to the ' \
                + new_branch + '_etl_load' + ' branch',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Merge ETL Task3 Branch to Data Load Branch')
def merge_etl_task3_branch(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    sourceBranch = repo.branch(new_branch + '_etl_task3')
    destinationBranch = repo.branch(new_branch + '_etl_load')
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + new_branch + '_etl_task3' + ' to the ' \
                + new_branch + '_etl_load' + ' branch',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='Merge Data Load Branch to ETL Branch')
def merge_etl_load_branch(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    sourceBranch = repo.branch(new_branch + '_etl_load')
    destinationBranch = repo.branch(new_branch)
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + new_branch + '_etl_load' + ' to the ' \
                + new_branch + ' branch',
        metadata={"committed_from": "prefect-operator",
                  '::lakefs::Prefect::url[url:ui]': variables.get('prefect_ui_endpoint') + '/flow-runs/flow-run/' \
                  + runtime.flow_run.id,
                 })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=variables.get('lakefs_ui_endpoint'), repo=variables.get("repo"))

@task(name='ETL Task1')
def etl_task1(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    branch = repo.branch(new_branch+'_etl_task1')    
    content = branch.object(path=variables.get("file_name")).reader(mode='r').read()
    if len(content.split(",")) < 1:
        raise Exception("Column _c0 not found in schema")
    else:
        return branch.object(path=variables.get("new_path") + '_c0/' + variables.get("file_name")).upload(mode='wb', data=content)

@task(name='ETL Task2_1')
def etl_task2_1(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    branch = repo.branch(new_branch+'_etl_task2')    
    content = branch.object(path=variables.get("file_name")).reader(mode='r').read()
    if len(content.split(",")) < 2:
        raise Exception("Column _c1 not found in schema struct<_c0:string>")
    else:
        return branch.object(path=variables.get("new_path") + '_c1/' + variables.get("file_name")).upload(mode='wb', data=content)
        
@task(name='ETL Task2_2')
def etl_task2_2(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    branch = repo.branch(new_branch+'_etl_task2')    
    content = branch.object(path=variables.get("file_name")).reader(mode='r').read()
    if len(content.split(",")) < 3:
        raise Exception("Column _c2 not found in schema struct<_c0:string,_c1:string>")
    else:
        return branch.object(path=variables.get("new_path") + '_c2/' + variables.get("file_name")).upload(mode='wb', data=content)
        
@task(name='ETL Task2_3')
def etl_task2_3(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    branch = repo.branch(new_branch+'_etl_task2')    
    content = branch.object(path=variables.get("file_name")).reader(mode='r').read()
    if len(content.split(",")) < 4:
        raise Exception("Column _c3 not found in schema struct<_c0:string,_c1:string,_c2:string>")
    else:
        return branch.object(path=variables.get("new_path") + '_c3/' + variables.get("file_name")).upload(mode='wb', data=content)
        
@task(name='ETL Task3')
def etl_task3(new_branch: str):
    repo = lakefs.repository(variables.get("repo"))
    branch = repo.branch(new_branch+'_etl_task3')    
    content = branch.object(path=variables.get("file_name")).reader(mode='r').read()
    if len(content.split(",")) < 5:
        raise Exception("Column _c4 not found in schema struct<_c0:string,_c1:string,_c2:string,_c3:string>")
    else:
        return branch.object(path=variables.get("new_path") + '_c4/' + variables.get("file_name")).upload(mode='wb', data=content)
       
@flow(name='lakeFS New DAG', flow_run_name=generate_flow_run_name)
def lakefs_new_dag():
    try:
        new_branch = variables.get('new_branch') + runtime.flow_run.scheduled_start_time.strftime("_%Y%m%dT%H%M%S")
        delete_demo_objects_future = delete_demo_objects.submit()
        create_etl_branch_future = create_etl_branch.submit(new_branch, wait_for=[delete_demo_objects_future])
        create_etl_load_branch_future = create_etl_load_branch.submit(new_branch, wait_for=[create_etl_branch_future])
        load_file_future = load_file.submit(new_branch, wait_for=[create_etl_load_branch_future])
        commit_load_future = commit_load.submit(new_branch, wait_for=[load_file_future])
        create_etl_task1_branch_future = create_etl_task1_branch.submit(new_branch, wait_for=[commit_load_future])
        create_etl_task2_branch_future = create_etl_task2_branch.submit(new_branch, wait_for=[commit_load_future])
        create_etl_task3_branch_future = create_etl_task3_branch.submit(new_branch, wait_for=[commit_load_future])
        etl_task1_future = etl_task1.submit(new_branch, wait_for=[create_etl_task1_branch_future])
        commit_task1_future = commit_task1.submit(new_branch, wait_for=[etl_task1_future])
        merge_etl_task1_branch_future = merge_etl_task1_branch.submit(new_branch, wait_for=[commit_task1_future])
        etl_task2_1_future = etl_task2_1.submit(new_branch, wait_for=[create_etl_task2_branch_future])
        commit_task2_1_future = commit_task2_1.submit(new_branch, wait_for=[etl_task2_1_future])
        etl_task2_2_future = etl_task2_2.submit(new_branch, wait_for=[commit_task2_1_future])
        commit_task2_2_future = commit_task2_2.submit(new_branch, wait_for=[etl_task2_2_future])
        etl_task2_3_future = etl_task2_3.submit(new_branch, wait_for=[commit_task2_2_future])
        commit_task2_3_future = commit_task2_3.submit(new_branch, wait_for=[etl_task2_3_future])
        merge_etl_task2_branch_future = merge_etl_task2_branch.submit(new_branch, wait_for=[commit_task2_3_future])
        etl_task3_future = etl_task3.submit(new_branch, wait_for=[create_etl_task3_branch_future])
        commit_task3_future = commit_task3.submit(new_branch, wait_for=[etl_task3_future])
        merge_etl_task3_branch_future = merge_etl_task3_branch.submit(new_branch, wait_for=[commit_task3_future])
        merge_etl_load_branch_future = merge_etl_load_branch.submit(new_branch,
                                                                    wait_for=[merge_etl_task1_branch_future,
                                                                              merge_etl_task2_branch_future,
                                                                              merge_etl_task3_branch_future])
        merge_etl_branch.submit(new_branch, wait_for=[merge_etl_load_branch_future])
        return runtime.flow_run.id
    except:
        return runtime.flow_run.id