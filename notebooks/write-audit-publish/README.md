# Write-Audit-Publish (WAP)

Write-Audit-Publish (WAP) is a pattern in data engineering to give greater control over data quality. It was popularised by Netflix back in 2017 in a talk by [Michelle Winters](https://www.linkedin.com/in/mufford/) at the DataWorks Summit called "[*Whoops the Numbers are wrong! Scaling Data Quality @ Netflix*](https://www.youtube.com/watch?v=fXHdeBnpXrg)". 

These notebooks can be used to experiment with the pattern implementations in different technologies. 

Please see the accompanying blog series for more details: 

1. [Data Engineering Patterns: Write-Audit-Publish (WAP)](https://lakefs.io/blog/data-engineering-patterns-write-audit-publish)
1. [How to Implement Write-Audit-Publish (WAP)](https://lakefs.io/blog/how-to-implement-write-audit-publish)
1. [Putting the Write-Audit-Publish Pattern into Practice with lakeFS](https://lakefs.io/blog/write-audit-publish-with-lakefs/)

## Usage

All of the notebooks except Nessie will run using the existing docker-compose.yml file that's in the root of the repository. 

For the Project Nessie notebook use the provided `docker-compose-nessie.yml` file: 

```bash
docker compose -f docker-compose-nessie.yml up
```
