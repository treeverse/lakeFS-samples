import os

def print_diff(diff):
    results = map(
        lambda n:[n.path,n.path_type,n.size_bytes,n.type],
        diff)

    from tabulate import tabulate
    print(tabulate(
        results,
        headers=['Path','Path Type','Size(Bytes)','Type']))

def print_commit(log):
    from datetime import datetime
    from pprint import pprint

    print('Message:', log.message)
    print('ID:', log.id)
    print('Committer:', log.committer)
    print('Creation Date:', datetime.utcfromtimestamp(log.creation_date).strftime('%Y-%m-%d %H:%M:%S'))
    print('Parents:', log.parents)
    print('Metadata:')
    pprint(log.metadata)

def lakefs_ui_endpoint(lakefsEndPoint):
    if lakefsEndPoint.startswith('http://host.docker.internal'):
        lakefsUIEndPoint = lakefsEndPoint.replace('host.docker.internal','127.0.0.1')
    elif lakefsEndPoint.startswith('http://lakefs'):
        lakefsUIEndPoint = lakefsEndPoint.replace('lakefs','127.0.0.1')
    else:
        lakefsUIEndPoint = lakefsEndPoint
        
    return lakefsUIEndPoint

def upload_objects(branch, local_path):
    for path, subdirs, files in os.walk(os.path.expanduser('~')+'/'+local_path):
        for file in files:
            if file.endswith(".jpg"):
                folder = path.rsplit("/")[-1]
                contentToUpload = open(path+'/'+file, 'rb').read() # Only a single file per upload which must be named \\\"content\\\"
                print(branch.object(local_path + '/' + folder + '/' + file).upload(data=contentToUpload, mode='wb', pre_sign=False))