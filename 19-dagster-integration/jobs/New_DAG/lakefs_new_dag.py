from dagster import job, op, in_process_executor, mem_io_manager, In, Nothing, Failure, resource, get_dagster_logger
from assets.lakefs_resources import lakefs_client_resource, lakefs_ui_endpoint, dagster_ui_endpoint
from lakefs_client import models
from lakefs_client.exceptions import NotFoundException
import os
from io import StringIO

my_logger = get_dagster_logger()

def print_commit_result(result, message, lakefs_ui_endpoint, repo):
    my_logger.info(message + result \
        + ' and lakeFS URL is: ' + lakefs_ui_endpoint \
        + '/repositories/' + repo + '/commits/' + result)

def print_branch_creation_result(result, message, lakefs_ui_endpoint, repo):
    my_logger.info(message + result \
        + ' and lakeFS URL is: ' + lakefs_ui_endpoint \
        + '/repositories/' + repo + '/objects?ref=' + result)

class NamedStringIO(StringIO):
    def __init__(self, content: str, name: str) -> None:
        super().__init__(content)
        self.name = name

@resource(config_schema={"repo": str, "sourceBranch": str, "newBranch": str, "newPath": str, "fileName": str})
def my_variables(init_context):
    return init_context.resource_config

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"})
def delete_demo_objects(context):
    path_list = models.PathList(
        paths=[
            context.resources.variables["fileName"],
            context.resources.variables["newPath"]+'_c0/'+context.resources.variables["fileName"],
            context.resources.variables["newPath"]+'_c1/'+context.resources.variables["fileName"],
            context.resources.variables["newPath"]+'_c2/'+context.resources.variables["fileName"],
            context.resources.variables["newPath"]+'_c3/'+context.resources.variables["fileName"],
            context.resources.variables["newPath"]+'_c4/'+context.resources.variables["fileName"],
        ],
    ) 
    
    context.resources.client.objects.delete_objects(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["sourceBranch"],
        path_list=path_list)
    
    response = context.resources.client.branches.diff_branch(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["sourceBranch"])
    if response.pagination.results > 0:
        commit = context.resources.client.commits.commit(
            repository=context.resources.variables["repo"],
            branch=context.resources.variables["sourceBranch"],
            commit_creation=models.CommitCreation(
                message='Deleted existing demo objects using dagster!',
                metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                          +context.run.run_id+'?selection="delete_demo_objects"&logs=query%3A"delete_demo_objects"',
                     }))
        print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                            lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])
    
@op(required_resource_keys={"variables","client","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def create_etl_branch(context):
    # Create the branch to run on
    branch_name = context.resources.variables["newBranch"]
    branch_id = context.resources.client.branches.create_branch(
        repository=context.resources.variables["repo"],
        branch_creation=models.BranchCreation(
            name=branch_name,
            source=context.resources.variables["sourceBranch"]))

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def create_etl_load_branch(context):
    # Create the branch to run on
    branch_name = context.resources.variables["newBranch"] + '_etl_load'
    branch_id = context.resources.client.branches.create_branch(
        repository=context.resources.variables["repo"],
        branch_creation=models.BranchCreation(
            name=branch_name,
            source=context.resources.variables["newBranch"]))    

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def create_etl_task1_branch(context):
    # Create the branch to run on
    branch_name = context.resources.variables["newBranch"] + '_etl_task1'
    branch_id = context.resources.client.branches.create_branch(
        repository=context.resources.variables["repo"],
        branch_creation=models.BranchCreation(
            name=branch_name,
            source=context.resources.variables["newBranch"] + '_etl_load'))    

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def create_etl_task2_branch(context):
    # Create the branch to run on
    branch_name = context.resources.variables["newBranch"] + '_etl_task2'
    branch_id = context.resources.client.branches.create_branch(
        repository=context.resources.variables["repo"],
        branch_creation=models.BranchCreation(
            name=branch_name,
            source=context.resources.variables["newBranch"] + '_etl_load'))    

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def create_etl_task3_branch(context):
    # Create the branch to run on
    branch_name = context.resources.variables["newBranch"] + '_etl_task3'
    branch_id = context.resources.client.branches.create_branch(
        repository=context.resources.variables["repo"],
        branch_creation=models.BranchCreation(
            name=branch_name,
            source=context.resources.variables["newBranch"] + '_etl_load'))    

    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def load_file(context):
    contentToUpload = open(os.getenv("DAGSTER_HOME")+'/'+context.resources.variables["fileName"], 'rb')
    object_stats = context.resources.client.objects.upload_object(
            repository=context.resources.variables["repo"],
            branch=context.resources.variables["newBranch"] + '_etl_load',
            path=context.resources.variables["fileName"],
            content=contentToUpload)

    my_logger.info(
        'lakeFS URL for the data file is: ' + context.resources.lakefs_ui_endpoint \
        + '/api/v1/repositories/' + context.resources.variables["repo"] + '/refs/' \
        + context.resources.variables["newBranch"] + '_etl_load' \
        + '/objects?path=' + context.resources.variables["fileName"])


@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_load(context):
    commit = context.resources.client.commits.commit(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["newBranch"] + '_etl_load',
        commit_creation=models.CommitCreation(
            message='committing etl_load to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="commit_load"&logs=query%3A"commit_load"',
                     }))
    print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_task1(context):
    commit = context.resources.client.commits.commit(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["newBranch"] + '_etl_task1',
        commit_creation=models.CommitCreation(
            message='committing etl_task1 to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="commit_task1"&logs=query%3A"commit_task1"',
                     }))
    print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_task2_1(context):
    commit = context.resources.client.commits.commit(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["newBranch"] + '_etl_task2',
        commit_creation=models.CommitCreation(
            message='committing _etl_task2_1 to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="commit_task2_1"&logs=query%3A"commit_task2_1"',
                     }))
    print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_task2_2(context):
    commit = context.resources.client.commits.commit(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["newBranch"] + '_etl_task2',
        commit_creation=models.CommitCreation(
            message='committing _etl_task2_2 to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="commit_task2_2"&logs=query%3A"commit_task2_2"',
                     }))
    print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_task2_3(context):
    commit = context.resources.client.commits.commit(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["newBranch"] + '_etl_task2',
        commit_creation=models.CommitCreation(
            message='committing commit_task2_3 to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="commit_task2_3"&logs=query%3A"commit_task2_3"',
                     }))
    print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_task3(context):
    commit = context.resources.client.commits.commit(
        repository=context.resources.variables["repo"],
        branch=context.resources.variables["newBranch"] + '_etl_task3',
        commit_creation=models.CommitCreation(
            message='committing commit_task3 to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="commit_task3"&logs=query%3A"commit_task3"',
                     }))
    print_commit_result(result=commit.id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def merge_etl_branch(context):
    mergeResult = context.resources.client.refs.merge_into_branch(
        repository=context.resources.variables["repo"],
        source_ref=context.resources.variables["newBranch"],
        destination_branch=context.resources.variables["sourceBranch"],
        merge=models.Merge(
            message='merging ' + context.resources.variables["newBranch"] + ' to the ' \
                + context.resources.variables["sourceBranch"] + ' branch',
            metadata={"committer": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="merge_etl_branch"&logs=query%3A"merge_etl_branch"',
                     }))
    print_commit_result(result=mergeResult.reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def merge_etl_task1_branch(context):
    mergeResult = context.resources.client.refs.merge_into_branch(
        repository=context.resources.variables["repo"],
        source_ref=context.resources.variables["newBranch"]+'_etl_task1',
        destination_branch=context.resources.variables["newBranch"]+'_etl_load',
        merge=models.Merge(
            message='merging ' + context.resources.variables["newBranch"]+'_etl_task1' + ' to the ' \
                + context.resources.variables["newBranch"]+'_etl_load' + ' branch',
            metadata={"committer": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="merge_etl_task1_branch"&logs=query%3A"merge_etl_task1_branch"',
                     }))
    print_commit_result(result=mergeResult.reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def merge_etl_task2_branch(context):
    mergeResult = context.resources.client.refs.merge_into_branch(
        repository=context.resources.variables["repo"],
        source_ref=context.resources.variables["newBranch"]+'_etl_task2',
        destination_branch=context.resources.variables["newBranch"]+'_etl_load',
        merge=models.Merge(
            message='merging ' + context.resources.variables["newBranch"]+'_etl_task2' + ' to the ' \
                + context.resources.variables["newBranch"]+'_etl_load' + ' branch',
            metadata={"committer": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="merge_etl_task2_branch"&logs=query%3A"merge_etl_task2_branch"',
                     }))
    print_commit_result(result=mergeResult.reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def merge_etl_task3_branch(context):
    mergeResult = context.resources.client.refs.merge_into_branch(
        repository=context.resources.variables["repo"],
        source_ref=context.resources.variables["newBranch"]+'_etl_task3',
        destination_branch=context.resources.variables["newBranch"]+'_etl_load',
        merge=models.Merge(
            message='merging ' + context.resources.variables["newBranch"]+'_etl_task3' + ' to the ' \
                + context.resources.variables["newBranch"]+'_etl_load' + ' branch',
            metadata={"committer": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="merge_etl_task3_branch"&logs=query%3A"merge_etl_task3_branch"',
                     }))
    print_commit_result(result=mergeResult.reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client","dagster_ui_endpoint","lakefs_ui_endpoint"},
    ins={"dependency1": In(Nothing), "dependency2": In(Nothing), "dependency3": In(Nothing)})
def merge_etl_load_branch(context):
    mergeResult = context.resources.client.refs.merge_into_branch(
        repository=context.resources.variables["repo"],
        source_ref=context.resources.variables["newBranch"]+'_etl_load',
        destination_branch=context.resources.variables["newBranch"],
        merge=models.Merge(
            message='merging ' + context.resources.variables["newBranch"]+'_etl_load' + ' to the ' \
                + context.resources.variables["newBranch"] + ' branch',
            metadata={"committer": "dagster-operator",
                     '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                      +context.run.run_id+'?selection="merge_etl_load_branch"&logs=query%3A"merge_etl_load_branch"',
                     }))
    print_commit_result(result=mergeResult.reference, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","client"}, ins={"start": In(Nothing)})
def etl_task1(context):
    contentObject = context.resources.client.objects.get_object(
        repository=context.resources.variables["repo"],
        ref=context.resources.variables["newBranch"]+'_etl_task1',
        path=context.resources.variables["fileName"])
    content = str(contentObject.read(), 'UTF-8')
    if len(content.split(",")) < 1:
        raise Failure(
            description="Column _c0 not found in schema")
    else:
        return context.resources.client.objects.upload_object(
                repository=context.resources.variables["repo"],
                branch=context.resources.variables["newBranch"] + '_etl_task1',
                path=context.resources.variables["newPath"] + '_c0/' + context.resources.variables["fileName"],
                content=NamedStringIO(content=content, name='content'))
        
@op(required_resource_keys={"variables","client"}, ins={"start": In(Nothing)})
def etl_task2_1(context):
    contentObject = context.resources.client.objects.get_object(
        repository=context.resources.variables["repo"],
        ref=context.resources.variables["newBranch"]+'_etl_task2',
        path=context.resources.variables["fileName"])
    content = str(contentObject.read(), 'UTF-8')
    if len(content.split(",")) < 2:
        raise Failure(
            description="Column _c1 not found in schema struct<_c0:string>")
    else:
        return context.resources.client.objects.upload_object(
                repository=context.resources.variables["repo"],
                branch=context.resources.variables["newBranch"] + '_etl_task2',
                path=context.resources.variables["newPath"] + '_c1/' + context.resources.variables["fileName"],
                content=NamedStringIO(content=content, name='content'))
        
@op(required_resource_keys={"variables","client"}, ins={"start": In(Nothing)})
def etl_task2_2(context):
    contentObject = context.resources.client.objects.get_object(
        repository=context.resources.variables["repo"],
        ref=context.resources.variables["newBranch"]+'_etl_task2',
        path=context.resources.variables["fileName"])
    content = str(contentObject.read(), 'UTF-8')
    if len(content.split(",")) < 3:
        raise Failure(
            description="Column _c2 not found in schema struct<_c0:string,_c1:string>")
    else:
        return context.resources.client.objects.upload_object(
                repository=context.resources.variables["repo"],
                branch=context.resources.variables["newBranch"] + '_etl_task2',
                path=context.resources.variables["newPath"] + '_c2/' + context.resources.variables["fileName"],
                content=NamedStringIO(content=content, name='content'))
        
@op(required_resource_keys={"variables","client"}, ins={"start": In(Nothing)})
def etl_task2_3(context):
    contentObject = context.resources.client.objects.get_object(
        repository=context.resources.variables["repo"],
        ref=context.resources.variables["newBranch"]+'_etl_task2',
        path=context.resources.variables["fileName"])
    content = str(contentObject.read(), 'UTF-8')
    if len(content.split(",")) < 4:
        raise Failure(
            description="Column _c3 not found in schema struct<_c0:string,_c1:string,_c2:string>")
    else:
        return context.resources.client.objects.upload_object(
                repository=context.resources.variables["repo"],
                branch=context.resources.variables["newBranch"] + '_etl_task2',
                path=context.resources.variables["newPath"] + '_c3/' + context.resources.variables["fileName"],
                content=NamedStringIO(content=content, name='content'))
        
@op(required_resource_keys={"variables","client"}, ins={"start": In(Nothing)})
def etl_task3(context):
    contentObject = context.resources.client.objects.get_object(
        repository=context.resources.variables["repo"],
        ref=context.resources.variables["newBranch"]+'_etl_task3',
        path=context.resources.variables["fileName"])
    content = str(contentObject.read(), 'UTF-8')
    if len(content.split(",")) < 5:
        raise Failure(
            description="Column _c4 not found in schema struct<_c0:string,_c1:string,_c2:string,_c3:string>")
    else:
        return context.resources.client.objects.upload_object(
                repository=context.resources.variables["repo"],
                branch=context.resources.variables["newBranch"] + '_etl_task3',
                path=context.resources.variables["newPath"] + '_c4/' + context.resources.variables["fileName"],
                content=NamedStringIO(content=content, name='content'))
        
@job(resource_defs={
    "io_manager": mem_io_manager,
    "variables": my_variables,
    "client": lakefs_client_resource,
    "lakefs_ui_endpoint": lakefs_ui_endpoint,
    "dagster_ui_endpoint": dagster_ui_endpoint
    },
    executor_def=in_process_executor)
def lakefs_new_dag():
    etl_branch_id = create_etl_branch(start=delete_demo_objects())
    etl_load_branch_id = create_etl_load_branch(start=etl_branch_id)
    object_stats = load_file(start=etl_load_branch_id)
    load_commit_id = commit_load(start=object_stats)
    etl_task1_branch_id = create_etl_task1_branch(start=load_commit_id)
    etl_task2_branch_id = create_etl_task2_branch(start=load_commit_id)
    etl_task3_branch_id = create_etl_task3_branch(start=load_commit_id)
    etl_task1_object_stats = etl_task1(start=etl_task1_branch_id)
    etl_task1_commit_id = commit_task1(start=etl_task1_object_stats)
    etl_task1_branch_merge_result = merge_etl_task1_branch(start=etl_task1_commit_id)
    etl_task2_1_object_stats = etl_task2_1(start=etl_task2_branch_id)
    etl_task2_1_commit_id = commit_task2_1(start=etl_task2_1_object_stats)
    etl_task2_2_object_stats = etl_task2_2(start=etl_task2_1_commit_id)
    etl_task2_2_commit_id = commit_task2_2(start=etl_task2_2_object_stats)
    etl_task2_3_object_stats = etl_task2_3(start=etl_task2_2_commit_id)
    etl_task2_3_commit_id = commit_task2_3(start=etl_task2_3_object_stats)
    etl_task2_branch_merge_result = merge_etl_task2_branch(start=etl_task2_3_commit_id)
    etl_task3_object_stats = etl_task3(start=etl_task3_branch_id)
    etl_task3_commit_id = commit_task3(start=etl_task3_object_stats)
    etl_task3_branch_merge_result = merge_etl_task3_branch(start=etl_task3_commit_id)
    etl_load_branch_merge_result = merge_etl_load_branch(
        dependency1=etl_task1_branch_merge_result,
        dependency2=etl_task2_branch_merge_result,
        dependency3=etl_task3_branch_merge_result)
    etl_branch_merge_result = merge_etl_branch(start=etl_load_branch_merge_result)
