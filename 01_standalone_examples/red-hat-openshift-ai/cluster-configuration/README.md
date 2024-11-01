# lakefs



## Getting started

LakeFS is a data versioning application that brings git-like versioning to object storage. It can interface with many object storage applications on the backend, and provide a S3 API gateway for object storage clients to connect to. In this demo, we'll configure OpenShift AI to connect over S3 interace to LakeFS, which will version the data in a backend Minio instance.

![lakefs](img/lakefs.png)

See the [slide deck](https://docs.google.com/presentation/d/1T9iOUt35AuppeQ-UB_hnnX8ufhCxzrNIn7ErmxRi-7k) for visuals and more details on the demo.

Repo of files used to bring up the LakeFS demo environment.

1. Bring up OpenShift clusters
2. Bring up OpenShift AI on the OpenShift cluster
3. Bring up Minio
4. Bring up LakeFS with [configuration](https://docs.lakefs.io/reference/configuration.html) to connect to Minio S3 Application
5. Configure OpenShift AI data connections to connect to LakeFS S3 gateway

You should now be able to run through the [Fraud Detection demo](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2.13/html/openshift_ai_tutorial_-_fraud_detection_example/index).

See [LakeFS documentation](https://docs.lakefs.io/) and [Minio documentation for OpenShift](https://min.io/docs/minio/kubernetes/openshift/index.html) for details.

## Files

- [minio-direct.yaml](./minio-direct.yaml): Bring up Minio as it is in the Fraud Detection demo, complete with configuring Minio storage buckets and the OpenShift AI data connections
- [minio-via-lakefs.yaml](./minio-via-lakefs.yaml): Bring up Minio as it is in the Fraud Detection demo, complete with configuring Minio storage buckets, but do NOT configure the OpenShift AI data connections
- [lakefs-local.yaml](./lakefs-local.yaml): Bring up LakeFS using local object storage.
- [lakefs-minio.yaml](./lakefs-minio.yaml): Bring up LakeFS configured to use Minio as backend object storage
