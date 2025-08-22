from dagster import job, op, in_process_executor, mem_io_manager, In, Nothing, repository, get_dagster_logger
from assets.lakefs_resources import lakefs_ui_endpoint, dagster_ui_endpoint
import lakefs
from jobs.Existing_DAG.lakefs_tutorial_taskflow_api_etl import lakefs_tutorial_taskflow_api_etl, my_variables

my_logger = get_dagster_logger()

def print_commit_result(result, message, lakefs_ui_endpoint, repo):
    my_logger.info(message + result \
        + ' and lakeFS URL is: ' + lakefs_ui_endpoint \
        + '/repositories/' + repo + '/commits/' + result)

def print_branch_creation_result(result, message, lakefs_ui_endpoint, repo):
    my_logger.info(message + result \
        + ' and lakeFS URL is: ' + lakefs_ui_endpoint \
        + '/repositories/' + repo + '/objects?ref=' + result)

@op(required_resource_keys={"variables","dagster_ui_endpoint","lakefs_ui_endpoint"})
def delete_demo_objects(context):
    branch = lakefs.repository(context.resources.variables["repo"]).branch(context.resources.variables["sourceBranch"])
    branch.object("total_order_value.txt").delete()
    
    for diff in branch.uncommitted():
        ref = branch.commit(
                message='Deleted existing demo objects using dagster!',
                metadata={"committed_from": "dagster-operator",
                         '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                         +context.run.run_id+'?selection=name%3A"delete_demo_objects"&logs=query%3Aname%3A"delete_demo_objects"',
                         })

        print_commit_result(result=ref.get_commit().id, message='lakeFS commit id is: ',
                            lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def create_etl_branch(context):
    branch_name = context.resources.variables["newBranch"]
    repo=lakefs.repository(context.resources.variables["repo"])
    branch = repo.branch(branch_name).create(source_reference=context.resources.variables["sourceBranch"])
    
    print_branch_creation_result(result=branch_name, message='lakeFS branch name is: ',
                                 lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables"}, ins={"start": In(Nothing)})
def trigger_existing_dag(context):
    lakefs_tutorial_taskflow_api_etl.execute_in_process(
        run_config=
            {
                "resources": 
                    {"variables": 
                        {"config": 
                            {"repo": context.resources.variables["repo"],
                             "sourceBranch": context.resources.variables["sourceBranch"],
                             "newBranch": context.resources.variables["newBranch"]
                            }
                        }
                    },
                "loggers": {"console": {"config": {"log_level": "INFO"}}},
            }
    )

@op(required_resource_keys={"variables","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def commit_etl_branch(context):
    branch = lakefs.repository(context.resources.variables["repo"]).branch(context.resources.variables["newBranch"])
    ref = branch.commit(
        message='committing to lakeFS using dagster!',
        metadata={"committed_from": "dagster-operator",
                 '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                 +context.run.run_id+'?selection=name%3A"commit_etl_branch"&logs=query%3Aname%3A"commit_etl_branch"',
                 })
    
    print_commit_result(result=ref.get_commit().id, message='lakeFS commit id is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])

@op(required_resource_keys={"variables","dagster_ui_endpoint","lakefs_ui_endpoint"}, ins={"start": In(Nothing)})
def merge_etl_branch(context):
    repo = lakefs.repository(context.resources.variables["repo"])
    sourceBranch = repo.branch(context.resources.variables["newBranch"])
    destinationBranch = repo.branch(context.resources.variables["sourceBranch"])
    mergeResult = sourceBranch.merge_into(
        destinationBranch,
        message='merging ' + context.resources.variables["newBranch"] + ' to the ' \
                + context.resources.variables["sourceBranch"] + ' branch',
        metadata={"committer": "dagster-operator",
                 '::lakefs::Dagster::url[url:ui]': context.resources.dagster_ui_endpoint+'/runs/' \
                  +context.run.run_id+'?selection=name%3A"merge_etl_branch"&logs=query%3Aname%3A"merge_etl_branch"',
                 })
    print_commit_result(result=mergeResult, message='lakeFS merge reference is: ',
                        lakefs_ui_endpoint=context.resources.lakefs_ui_endpoint, repo=context.resources.variables["repo"])
    
@job(resource_defs={
    "io_manager": mem_io_manager,
    "variables": my_variables,
    "lakefs_ui_endpoint": lakefs_ui_endpoint,
    "dagster_ui_endpoint": dagster_ui_endpoint
    },
    executor_def=in_process_executor)
def lakefs_wrapper_dag():
    branch_id = create_etl_branch(start=delete_demo_objects())
    job_result = trigger_existing_dag(start=branch_id)
    commit = commit_etl_branch(start=job_result)
    merge_etl_branch(start=commit)

@repository
def repository_lakefs_existing_dag():
    return [
        lakefs_tutorial_taskflow_api_etl,
        lakefs_wrapper_dag,
    ]