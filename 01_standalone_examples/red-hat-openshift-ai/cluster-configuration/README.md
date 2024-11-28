# Overview

[lakeFS](https://lakefs.io/) is a data versioning application that brings git-like versioning to object storage. It can interface with many object storage applications on the backend, and provide a S3 API gateway for object storage clients to connect to. In this demo, we'll configure OpenShift AI to connect over S3 interace to lakeFS, which will version the data in a backend [MinIO](https://min.io/docs/minio/kubernetes/openshift/index.html) instance.

![lakefs](img/lakefsv3.png)

# lakeFS with OpenShift AI Demo

The following steps should be followed to perform the [Fraud Detection demo](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2-latest/html/openshift_ai_tutorial_-_fraud_detection_example/index) on OpenShift AI, with lakeFS used for object storage management.

## Prerequisites

1. Bring up [OpenShift cluster](https://docs.redhat.com/en/documentation/openshift_container_platform/4.17#Install)
2. Install [OpenShift Service Mesh](https://docs.openshift.com/container-platform/4.16/service_mesh/v2x/installing-ossm.html#ossm-install-ossm-operator_installing-ossm), [OpenShift Serverless](https://docs.openshift.com/serverless/1.34/install/install-serverless-operator.html) and [OpenShift Pipelines](https://docs.openshift.com/pipelines/1.16/install_config/installing-pipelines.html) on the OpenShift cluster
3. Install [OpenShift AI](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2.13/html/installing_and_uninstalling_openshift_ai_self-managed/index) on the OpenShift cluster
4. Install the `oc` OpenShift [CLI client](https://docs.openshift.com/container-platform/4.16/cli_reference/openshift_cli/getting-started-cli.html) on a machine thas access to the cluster

## Deploy and Configure the Environment
From the client machine, authenticate the `oc` client.

```
oc login <cluster_api_url> -u kubeadmin -p <admin_pw>
```

### Create a `lakefs` project in OpenShift.

```
oc new-project lakefs
```

### Clone the lakeFS samples repo
Clone the [lakeFS-samples.git](https://github.com/treeverse/lakeFS-samples.git) repository and change into the newly created directory.

```
git clone https://github.com/treeverse/lakeFS-samples.git

cd lakeFS-samples/red-hat-openshift-ai/cluster-configuration
```

### Deploy MinIO
Deploy MinIO in the `lakefs` project using the `minio-via-lakefs.yaml` file.

```
oc apply -f minio-via-lakefs.yaml
```
A random MinIO root user and password will be generated, stored in a `secret`, and used to populate MinIO with three storage buckets:
* **my-storage** 
* **pipeline artifacts**
* **quickstart**


### Deploy lakeFS
Deploy lakeFS in the **lakefs** project using the `lakefs-minio.yaml` file. This yaml will not only deploy lakefs but also:
* connect it with MinIO buckets created earlier
* create two lakeFS repo:
  * **quickstart:** as a sample data repo
  * **my-storage** which is connected to backed my-storage s3 bucket created earlier



```
oc apply -f lakefs-minio.yaml
```

### Access lakeFS UI
You can now log into the OpenShift cluster's web console as a regular user (ie. developer). Follow the arrows in the screenshot below to find the lakeFS `route`, which provides external access to the lakeFS administrator. Use the lakeFS route to access the lakeFS UI.

For this demo, you will use the following credentials to access the lakeFS UI.

* **User**: admin
* **Access Key**: something
* **Secret Access Key**: simple

![lakefs](img/lakefs-route.png)

NOTES:
- You can also follow those steps, but click on MinIO in the topology, to find the `route` to access MinIO's console or S3 interface. MinIO access credentials can be found in the `minio-root-user` secret within the OpenShift web console when logged in as an admin user (ie. kubeadmin).

  - Switch to the **Administrator** persona using the drop-down at the top left
  - Expand the **Workloads** navigation
  - Click on **Secrets**
  - Filter for 'minio' name
  - Click on the **minio-root-user** secret
  - Scroll down and click on **Reveal values** to see the MinIO root user and password

- If you don't see the visual layout as shown in the screenshot, then click on the icon highlighted below to change the view.

![lakefs](img/topology.png)

### Access OpenShift AI Console
From the OpenShift web console, you can now open the OpenShift AI web console as shown below.

![lakefs](img/oai-console.png)

### Create a Data Science Project
After logging in to the OpenShift AI web console, follow the arrows below to create a new Data Science Project called **lakefs** if it was not automatically created.

![lakefs](img/project.png)


## Fraud Detection Demo

You may now run through the [Fraud Detection demo](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2-latest/html/openshift_ai_tutorial_-_fraud_detection_example/index) in the new **lakefs** data science project. 

NOTES: 
* Use the `lakefs` data science project for the demo. You do not need to create a new project.
* When going through the demo, follow the steps to manually configure the storage data connections. **Do not** follow steps that use a script to automate the MinIO storage deployment, configuration and data connections. 
* When cloning the notebooks from the git repo in to the workbench, be sure to pull them from the `lakeFS-samples` repo as the notebooks there have been modified from the default notebooks to incorporate lakeFS. Do not use the notebook git repos shown in the demo. 


See [lakeFS documentation](https://docs.lakefs.io/) and [MinIO documentation for OpenShift](https://min.io/docs/minio/kubernetes/openshift/index.html) for details.

# File Descriptions

- [lakefs-local.yaml](./lakefs-local.yaml): Bring up lakeFS using local object storage. This would be useful for a quick demo where MinIO is not included.
- [lakefs-minio.yaml](./lakefs-minio.yaml): Bring up lakeFS configured to use MinIO as backend object storage. This will be used in the lakeFS demo.
- [minio-direct.yaml](./minio-direct.yaml): This file would only be used if lakeFS is not in the picture and OpenShift AI will communicate directly with MinIO. It will bring up MinIO as it is in the default Fraud Detection demo, complete with configuring MinIO storage buckets and the OpenShift AI data connections. It may serve useful in debugging an issue.
- [minio-via-lakefs.yaml](./minio-via-lakefs.yaml): Bring up MinIO for the modified Fraud Detection demo that includes lakeFS, complete with configuring MinIO storage buckets, but do NOT configure the OpenShift AI data connections. This will be used in the lakeFS demo.
