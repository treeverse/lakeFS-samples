from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

def print_branches(list_branches):
    results = map(
        lambda n:[n.id,n.commit_id],
        list_branches.results)

    from tabulate import tabulate
    print(tabulate(
        results,
        headers=['id','commit_id']))
    
def print_diff_refs(diff_refs):
    results = map(
        lambda n:[n.path,n.path_type,n.size_bytes,n.type],
        diff_refs.results)

    from tabulate import tabulate
    print(tabulate(
        results,
        headers=['Path','Path Type','Size(Bytes)','Type']))

def upload_object(branch, local_path, filename, lakefs_path):
    if local_path == '':
        filename_with_path = os.path.expanduser('~')+'/'+filename
    else:
        filename_with_path = os.path.expanduser('~')+'/'+local_path+'/'+filename
        
    if lakefs_path == '':
        lakefs_filename_with_path = filename
    else:
        lakefs_filename_with_path = lakefs_path + '/' + filename
        
    contentToUpload = open(filename_with_path, 'r').read() # Only a single file per upload which must be named \\\"content\\\"
    print(branch.object(lakefs_filename_with_path).upload(data=contentToUpload, mode='wb', pre_sign=False))


from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.db import provide_session
from airflow.models import XCom

# The execution context and any results are automatically passed by task.post_execute method
def print_commit_result(context, result, message):
    LoggingMixin().log.info(message + result \
        + ' and lakeFS URL is: ' + Variable.get("lakefsUIEndPoint") \
        + '/repositories/' + Variable.get("repo") + '/commits/' + result)

@provide_session
def override_lakefs_link(context, result, session=None):
    if Variable.get("lakefsEndPoint").startswith('http://host.docker.internal'):
        session.query(XCom) \
        .filter(XCom.dag_id == context["dag"].dag_id, \
                XCom.task_id == context["task"].task_id, \
                XCom.execution_date == context['execution_date'], \
                XCom.key == 'lakefs_commit') \
        .delete(synchronize_session='fetch')

        value = {'base_url': Variable.get("lakefsUIEndPoint"), 'repo': Variable.get("repo"), 'commit_digest': result}
        LoggingMixin().log.info(f"Overridden Persist lakeFS commit data {value}")    
        context["ti"].xcom_push(key='lakefs_commit', value=value)

# The execution context and any results are automatically passed by task.post_execute method
def post_execute_commit(context, result, message):
    override_lakefs_link(context, result)
    print_commit_result(context, result, message)
    
