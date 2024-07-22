from airflow.decorators import dag
#from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator
import lakefs
from airflow.models import Variable
from airflow.models.dagrun import DagRun
import time
from functools import partial
from airflow.utils.log.logging_mixin import LoggingMixin

import sys
sys.path.insert(0, '/home/jovyan')
from lakefs_demo import print_commit_result, post_execute_commit

repo = Variable.get("repo")
lakefsAccessKey = Variable.get("lakefsAccessKey")
lakefsSecretKey = Variable.get("lakefsSecretKey")
lakefsEndPoint = Variable.get("lakefsEndPoint")

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "lakeFS",
    "branch": Variable.get("newBranch") + '_{{ ts_nodash }}',
    "repo": Variable.get("repo"),
    #"path": Variable.get("fileName"),
    "default-branch": Variable.get("sourceBranch"),
    "lakefs_conn_id": Variable.get("conn_lakefs")
}

@dag(default_args=default_args,
     render_template_as_native_obj=True,
     max_active_runs=1,
     start_date=days_ago(2),
     schedule_interval=None,
     tags=['testing'])
def lakefs_iceberg_dag():
    jars_partition_data = Variable.get("spark_home") + '/jars/hadoop-aws-*.jar,' + Variable.get("spark_home") + '/jars/aws-java-sdk-bundle-*.jar'
    sparkConfig ={
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": lakefsEndPoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": lakefsAccessKey,
        "spark.hadoop.fs.s3a.secret.key": lakefsSecretKey,
        "spark.sql.catalog.lakefs": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lakefs.catalog-impl": "io.lakefs.iceberg.LakeFSCatalog",
        "spark.sql.catalog.lakefs.warehouse": f"lakefs://{repo}",
        "spark.sql.catalog.lakefs.uri": lakefsEndPoint,
        "spark.sql.catalog.lakefs.cache-enabled": "false",
        "spark.sql.defaultCatalog": "lakefs",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    }
    sparkPackages = "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.0,io.lakefs:lakefs-iceberg:0.1.1"
    applicationPath="/home/jovyan/airflow/Iceberg/"
    
    # Create the branch to run on
    task_create_etl_branch = LakeFSCreateBranchOperator(
        task_id='create_etl_branch',
        branch=default_args.get('branch'),
        source_branch=default_args.get('default-branch')
    )
    
    task_create_etl_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_tables_branch = LakeFSCreateBranchOperator(
        task_id='create_tables_branch',
        branch=default_args.get('branch') + '_create_tables',
        source_branch=default_args.get('branch')
    )

    task_create_tables_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_authors_table_branch = LakeFSCreateBranchOperator(
        task_id='create_authors_table_branch',
        branch=default_args.get('branch') + '_create_authors_table',
        source_branch=default_args.get('branch') + '_create_tables'
    )

    task_create_authors_table_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_books_table_branch = LakeFSCreateBranchOperator(
        task_id='create_books_table_branch',
        branch=default_args.get('branch') + '_create_books_table',
        source_branch=default_args.get('branch') + '_create_tables'
    )

    task_create_books_table_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_book_sales_table_branch = LakeFSCreateBranchOperator(
        task_id='create_book_sales_table_branch',
        branch=default_args.get('branch') + '_create_book_sales_table',
        source_branch=default_args.get('branch') + '_create_tables'
    )

    task_create_book_sales_table_branch.post_execute = partial(print_commit_result, message='lakeFS commit id is: ')

    task_create_authors_table = SparkSubmitOperator(
        task_id='create_authors_table',
        conn_id='conn_spark',
        application=applicationPath+"create_authors_table.py",
        application_args=[default_args.get('branch') + '_create_authors_table'],
        jars=jars_partition_data,
        packages=sparkPackages,
        conf=sparkConfig
    )
    
    task_create_authors_table.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_create_authors_table' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_create_authors_table' )

    task_commit_authors_table = LakeFSCommitOperator(
        task_id='commit_authors_table',
        branch=default_args.get('branch') + '_create_authors_table',
        msg='committ Authors table to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator", "source": "Sales System"}
    )

    task_commit_authors_table.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_create_books_table = SparkSubmitOperator(
        task_id='create_books_table',
        conn_id='conn_spark',
        application=applicationPath+"create_books_table.py",
        application_args=[default_args.get('branch') + '_create_books_table'],
        jars=jars_partition_data,
        packages=sparkPackages,
        conf=sparkConfig
    )
    
    task_create_books_table.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_create_books_table' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_create_books_table' )

    task_commit_books_table = LakeFSCommitOperator(
        task_id='commit_books_table',
        branch=default_args.get('branch') + '_create_books_table',
        msg='committ Books table to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator", "source": "Sales System"}
    )

    task_commit_books_table.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_create_book_sales_table = SparkSubmitOperator(
        task_id='create_book_sales_table',
        conn_id='conn_spark',
        application=applicationPath+"create_book_sales_table.py",
        application_args=[default_args.get('branch') + '_create_book_sales_table'],
        jars=jars_partition_data,
        packages=sparkPackages,
        conf=sparkConfig
    )
    
    task_create_book_sales_table.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] + '_create_book_sales_table' \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] + '_create_book_sales_table' )

    task_commit_book_sales_table = LakeFSCommitOperator(
        task_id='commit_book_sales_table',
        branch=default_args.get('branch') + '_create_book_sales_table',
        msg='committ Book Sales table to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator", "source": "Sales System"}
    )

    task_commit_book_sales_table.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_merge_create_authors_table_branch = LakeFSMergeOperator(
        task_id='merge_create_authors_table_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch') + '_create_authors_table',
        destination_branch=default_args.get('branch') + '_create_tables',
        msg='merging ' + default_args.get('branch') + '_create_authors_table' + ' to the ' + default_args.get('branch') + '_create_tables' + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_create_authors_table_branch.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_merge_create_books_table_branch = LakeFSMergeOperator(
        task_id='merge_create_books_table_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch') + '_create_books_table',
        destination_branch=default_args.get('branch') + '_create_tables',
        msg='merging ' + default_args.get('branch') + '_create_books_table' + ' to the ' + default_args.get('branch') + '_create_tables' + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_create_books_table_branch.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_merge_create_book_sales_table_branch = LakeFSMergeOperator(
        task_id='merge_create_book_sales_table_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch') + '_create_book_sales_table',
        destination_branch=default_args.get('branch') + '_create_tables',
        msg='merging ' + default_args.get('branch') + '_create_book_sales_table' + ' to the ' + default_args.get('branch') + '_create_tables' + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_create_book_sales_table_branch.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_merge_create_tables_branch = LakeFSMergeOperator(
        task_id='merge_create_tables_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch') + '_create_tables',
        destination_branch=default_args.get('branch'),
        msg='merging ' + default_args.get('branch') + '_create_tables' + ' to the ' + default_args.get('branch') + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_create_tables_branch.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    task_delete_cancelled_sales = SparkSubmitOperator(
        task_id='delete_cancelled_sales',
        conn_id='conn_spark',
        application=applicationPath+"delete_cancelled_sales.py",
        application_args=[default_args.get('branch')],
        jars=jars_partition_data,
        packages=sparkPackages,
        conf=sparkConfig
    )
    
    task_delete_cancelled_sales.pre_execute = lambda context: LoggingMixin().log.info(
        'Branch name is: ' + Variable.get("newBranch") + '_' \
        + context['ts_nodash'] \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/objects?ref=' \
        + Variable.get("newBranch") + '_' + context['ts_nodash'] )

    task_commit_cancelled_sales = LakeFSCommitOperator(
        task_id='commit_cancelled_sales',
        branch=default_args.get('branch'),
        msg='committ Cancelled Sales to lakeFS using airflow!',
        metadata={"committed_from": "airflow-operator", "source": "Data Cleansing Process"}
    )

    task_commit_cancelled_sales.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    # Merge the changes back to the main branch.
    task_merge_etl_branch = LakeFSMergeOperator(
        task_id='merge_etl_branch',
        do_xcom_push=True,
        source_ref=default_args.get('branch'),
        destination_branch=default_args.get('default-branch'),
        msg='merging ' + default_args.get('branch') + ' to the ' + default_args.get('default-branch') + ' branch',
        metadata={"committer": "airflow-operator"}
    )

    task_merge_etl_branch.post_execute = partial(post_execute_commit, message='lakeFS commit id is: ')

    
    task_create_etl_branch >> task_create_tables_branch >> [task_create_authors_table_branch, task_create_books_table_branch, task_create_book_sales_table_branch]
    
    task_create_authors_table_branch >> task_create_authors_table >> task_commit_authors_table >> task_merge_create_authors_table_branch >> task_merge_create_tables_branch
    task_create_books_table_branch >> task_create_books_table >> task_commit_books_table >> task_merge_create_books_table_branch >> task_merge_create_tables_branch
    task_create_book_sales_table_branch >> task_create_book_sales_table >> task_commit_book_sales_table >> task_merge_create_book_sales_table_branch >> task_merge_create_tables_branch
    
    task_merge_create_tables_branch >> task_delete_cancelled_sales >> task_commit_cancelled_sales >> task_merge_etl_branch
    

sample_workflow_dag = lakefs_iceberg_dag()