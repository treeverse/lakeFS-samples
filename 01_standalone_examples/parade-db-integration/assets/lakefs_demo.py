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

def use_ssl(lakefsEndPoint):
    if lakefsEndPoint.startswith('https'):
        return "true"
    else:
        return "false"

def lakefs_endpoint_for_paradedb(lakefsEndPoint):
    lakefsDBEndPoint = lakefsEndPoint.replace('http://lakefs:8000','host.docker.internal:8007')
    lakefsDBEndPoint = lakefsDBEndPoint.replace('https://','')
    lakefsDBEndPoint = lakefsDBEndPoint.replace('http://','')
        
    return lakefsDBEndPoint