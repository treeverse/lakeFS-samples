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

def upload_object(client, repo_name, branch_name, local_path, filename, lakefs_path):
    if local_path == '':
        filename_with_path = os.path.expanduser('~')+'/'+filename
    else:
        filename_with_path = os.path.expanduser('~')+'/'+local_path+'/'+filename
        
    if lakefs_path == '':
        lakefs_filename_with_path = filename
    else:
        lakefs_filename_with_path = lakefs_path + '/' + filename
        
    contentToUpload = open(filename_with_path, 'rb') # Only a single file per upload which must be named \\\"content\\\"
    print(client.objects.upload_object(
        repository=repo_name,
        branch=branch_name,
        path=lakefs_filename_with_path, content=contentToUpload))
