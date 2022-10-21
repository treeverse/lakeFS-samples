from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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
