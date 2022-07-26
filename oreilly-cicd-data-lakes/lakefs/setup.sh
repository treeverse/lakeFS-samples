#!/bin/sh
echo "⌛️ setting up the O'Reilly lakeFS Challenge..."
lakectl fs ls lakefs://oreilly-challenge/main/ >/dev/null 2>&1
if [ $? -eq 0 ]; then
    cat /data/banner.txt
    exit 0
fi


lakefs setup --user-name challenger --access-key-id AKIAIOSFODNN7EXAMPLE --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY >/dev/null
lakectl repo create lakefs://oreilly-challenge s3://oreilly-challenge-storage >/dev/null

# Upload data:
echo "📂 Downloading challenge datasets..."
export LAKECTL_BASE_URI="lakefs://oreilly-challenge/"

# Instructions
lakectl fs upload --source <(zcat /data/int.gz) main/instructions.txt > /dev/null
lakectl commit main -m "initial commit: Added instructions!" --meta "format=text" >/dev/null

# Download and set up datasets:
lakectl branch create ingest --source main >/dev/null

# users dataset
lakectl fs upload --recursive --source /data/users ingest/datasets/users >/dev/null
lakectl commit ingest -m "Initial users ETL" --meta "format=parquet" --meta "src=operational_mysql" >/dev/null

# user events
lakectl fs upload --recursive --source /data/user_events1 ingest/datasets/user_events >/dev/null
lakectl commit ingest -m "Initial events ETL" --meta "format=parquet" --meta "src=operational_mysql" --meta "partitioned_by=EventType" --meta "compression=SNAPPY" >/dev/null

# merge both into main
lakectl merge ingest main >/dev/null

# fix events data!
lakectl branch create improvedDataJune2020 --source main >/dev/null
lakectl fs rm --recursive improvedDataJune2020/datasets/user_events/ >/dev/null
lakectl fs upload --recursive --source /data/user_events2 improvedDataJune2020/datasets/user_events >/dev/null
lakectl commit improvedDataJune2020 -m "Fixed events ETL bug" --meta "format=parquet" --meta "src=operational_mysql" --meta "partitioned_by=EventType" --meta "compression=SNAPPY" >/dev/null

# Hide the treasure
lakectl branch create prizes --source main >/dev/null
lakectl fs upload --recursive --source /data/treasure_chests prizes/datasets/treasure_chests >/dev/null
lakectl commit prizes -m "There is a key hiding in here, somewhere!" --meta "format=parquet" --meta "src=magic" --meta "compression=SNAPPY" >/dev/null

echo "✅ Done!"
echo ""

cat /data/banner.txt