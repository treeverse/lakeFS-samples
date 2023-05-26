from dagster import job, op, get_dagster_logger, in_process_executor, mem_io_manager, Config, RunConfig
from assets.lakefs_client_creation import create_lakefs_client
from assets.lakefs_ops_config import LakeFSOpConfig
from lakefs_client import models
from jobs.Existing_DAG.lakefs_tutorial_taskflow_api_etl import lakefs_tutorial_taskflow_api_etl

@op
def create_etl_branch(client, config: LakeFSOpConfig):
    # Create the branch to run on
    return client.branches.create_branch(
        repository=config.repo,
        branch_creation=models.BranchCreation(
            name=config.newBranch,
            source=config.sourceBranch))    

@op
def trigger_existing_dag(branch_id, config: LakeFSOpConfig):
    return lakefs_tutorial_taskflow_api_etl.execute_in_process(
        run_config=RunConfig(
            {
                "load": LakeFSOpConfig(repo=config.repo, sourceBranch=config.sourceBranch, newBranch=config.newBranch),
            }
        )
    )

@op
def commit_etl_branch(job_result, client, config: LakeFSOpConfig):
    return client.commits.commit(
        repository=config.repo,
        branch=config.newBranch,
        commit_creation=models.CommitCreation(
            message='committing to lakeFS using dagster!',
            metadata={"committed_from": "dagster-operator"}))

@op
def merge_etl_branch(commit, client, config: LakeFSOpConfig):
    client.refs.merge_into_branch(
        repository=config.repo,
        source_ref=config.newBranch, 
        destination_branch=config.sourceBranch)

@job(resource_defs={"io_manager": mem_io_manager}, executor_def=in_process_executor)
def lakefs_wrapper_dag():
    client = create_lakefs_client()
    branch_id = create_etl_branch(client)
    job_result = trigger_existing_dag(branch_id)
    commit = commit_etl_branch(job_result, client)
    merge_etl_branch(commit, client)
