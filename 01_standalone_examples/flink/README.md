# Integration of lakeFS with Apache Flink

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes lakeFS with Flink which you can run on your local machine.

## Let's Get Started 👩🏻‍💻

Clone this repository

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/flink
   ```

You now have two options: 

### **Run a Flink server with your existing lakeFS Server**

If you have already [installed lakeFS](https://docs.lakefs.io/deploy/) or are utilizing [lakeFS cloud](https://lakefs.cloud/) then follow these steps:

   1. lakeFS uses [S3 Gateway](https://docs.lakefs.io/understand/architecture.html#s3-gateway) to communicate with Flink. So, change `fs.s3a.endpoint`, `fs.s3a.access.key` and `fs.s3a.secret.key` Flink properties for `jobmanager` and `taskmanager` services in `docker-compose.yml` file to lakeFS endpoint e.g. `https://username.aws_region_name.lakefscloud.io`(if you are using lakeFS Cloud), lakeFS Access Key and lakeFS Secret Key:

      ```bash
      FLINK_PROPERTIES=
      jobmanager.rpc.address: jobmanager  
      state.backend: filesystem 
      fs.s3a.path.style.access: true
      fs.s3a.endpoint: http://lakefs:8000
      fs.s3a.access.key: AKIAIOSFOLKFSSAMPLES
      fs.s3a.secret.key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      ```

   2. Run only Flink server:

      ```bash
      docker compose up 
      ```

### **Don't have a lakeFS Server or Object Store?**

If you want to provision a lakeFS server as well as MinIO for your object store, plus Flink then bring up the full stack:

   ```bash
   docker compose --profile local-lakefs up
   ```

### URLs and login details

* Flink Dashboard http://localhost:8081/

If you've brought up the full stack you'll also have: 

* LakeFS http://localhost:38000/ (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO http://localhost:39001/ (`minioadmin`/`minioadmin`)


## Demo Instructions

To deploy Flink's example Word Count job to the running Flink server, issue the following command. This job will read the `README.md` file from the `main` branch of `quickstart` lakeFS repository and will write the output back to `word-count` folder in the same lakeFS repository & branch. If you want to use another lakeFS repository/branch or another text file then change the command accordingly:

   ```bash
   docker exec -it lakefs-with-flink-jobmanager \
   ./bin/flink run examples/streaming/WordCount.jar \
   --input s3://quickstart/main/README.md \
   --output s3://quickstart/main/word-count 
   ```
