import fileinput
import os

def lakefs_create_dag(client, repo, sourceBranch, newBranch, dags_folder_on_lakefs, dag_template_filename, dag_name):
    dag_template = client.objects.get_object(
        repository=repo,
        ref=sourceBranch,
        path=dags_folder_on_lakefs+'/'+dag_template_filename)
    
    dag_id = dag_name + '.' + newBranch

    new_filename = os.environ['HOME'] + '/airflow/dags/' + dag_id + '.py'
    with open(new_filename, 'w') as f:
        f.write(str(dag_template.read(), 'UTF-8'))

    with fileinput.input(new_filename, inplace=True) as file:
        for line in file:
            new_line = line.replace('branch_name', "'" + newBranch + "'")
            print(new_line, end='')
