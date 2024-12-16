import os
import pickle
import boto3
import botocore

import pyarrow
import pyarrow.fs
import pyarrow.csv

import sklearn
import numpy as np

import tensorflow as tf
import onnx
import tf2onnx
from keras.models import Sequential
from keras.layers import Dense, Dropout, BatchNormalization, Activation

import ray
from ray import train
from ray.train import RunConfig, ScalingConfig
from ray.train.tensorflow import TensorflowTrainer
from ray.train.tensorflow.keras import ReportCheckpointCallback
from ray.data.preprocessors import Concatenator, StandardScaler

use_gpu = os.environ.get("USE_GPU", "False").lower() == "true"
num_workers = int(os.environ.get("NUM_WORKERS", "1"))
num_epochs = int(os.environ.get("NUM_EPOCHS", "2"))
batch_size = int(os.environ.get("BATCH_SIZE", "64"))
learning_rate = 1e-3
output_column_name = "features"

feature_columns = [
    "distance_from_last_transaction",
    "ratio_to_median_purchase_price",
    "used_chip",
    "used_pin_number",
    "online_order",
]

label_columns = [
    "fraud",
]

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
endpoint_url = os.environ.get("AWS_S3_ENDPOINT")
region_name = os.environ.get("AWS_DEFAULT_REGION")
bucket_name = os.environ.get("AWS_S3_BUCKET")

pipeline_artifacts_access_key_id = os.environ.get("PIPELINE_ARTIFACTS_ACCESS_KEY_ID")
pipeline_artifacts_secret_access_key = os.environ.get("PIPELINE_ARTIFACTS_SECRET_ACCESS_KEY")
pipeline_artifacts_endpoint_url = os.environ.get("PIPELINE_ARTIFACTS_ENDPOINT_URL")
pipeline_artifacts_bucket_name = os.environ.get("PIPELINE_ARTIFACTS_S3_BUCKET")

trainingBranch = "train01"
train_data = os.environ.get("TRAIN_DATA", f"{trainingBranch}/data/train.csv")

keras_model_filename = "model.keras"
model_output_prefix = os.environ.get("MODEL_OUTPUT", f"{trainingBranch}/models/fraud/1/")
model_output_filename = os.environ.get("MODEL_OUTPUT_FILENAME", "model.onnx")
scaler_output = model_output_prefix + "scaler.pkl"
model_output = model_output_prefix + model_output_filename


def get_pyarrow_fs():
    return pyarrow.fs.S3FileSystem(
        access_key=aws_access_key_id,
        secret_key=aws_secret_access_key,
        region=region_name,
        endpoint_override=endpoint_url)

def get_pipeline_artifacts_fs():
    return pyarrow.fs.S3FileSystem(
        access_key=pipeline_artifacts_access_key_id,
        secret_key=pipeline_artifacts_secret_access_key,
        region=region_name,
        endpoint_override=pipeline_artifacts_endpoint_url)

def get_s3_resource():
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)

    s3_resource = session.resource(
        's3',
        config=botocore.client.Config(signature_version='s3v4'),
        endpoint_url=endpoint_url,
        region_name=region_name)

    return s3_resource

def get_pipeline_artifacts_s3_resource():
    session = boto3.session.Session(
        aws_access_key_id=pipeline_artifacts_access_key_id,
        aws_secret_access_key=pipeline_artifacts_secret_access_key)

    s3_resource = session.resource(
        's3',
        config=botocore.client.Config(signature_version='s3v4'),
        endpoint_url=pipeline_artifacts_endpoint_url,
        region_name=region_name)

    return s3_resource

def get_class_weights(pyarrow_fs):
    with pyarrow_fs.open_input_file(f"{bucket_name}/{train_data}") as file:
        training_table = pyarrow.csv.read_csv(file)

    y_train = training_table.to_pandas()
    y_train = y_train.loc[:, label_columns]
    # Since the dataset is unbalanced (it has many more non-fraud transactions than fraudulent ones), set a class weight to weight the few fraudulent transactions higher than the many non-fraud transactions.
    class_weights = sklearn.utils.class_weight.compute_class_weight(
        'balanced',
        classes=np.unique(y_train),
        y=y_train.values.ravel())
    class_weights = {i : class_weights[i] for i in range(len(class_weights))}

    return class_weights


def build_model() -> tf.keras.Model:
    model = Sequential()
    model.add(Dense(32, activation='relu', input_dim=len(feature_columns)))
    model.add(Dropout(0.2))
    model.add(Dense(32))
    model.add(BatchNormalization())
    model.add(Activation('relu'))
    model.add(Dropout(0.2))
    model.add(Dense(32))
    model.add(BatchNormalization())
    model.add(Activation('relu'))
    model.add(Dropout(0.2))
    model.add(Dense(1, activation='sigmoid'))
    return model


def train_func(config: dict):
    batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)
    cw = config.get("class_weight", 3)

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        multi_worker_model = build_model()
        multi_worker_model.compile(
            optimizer="adam",
            loss="binary_crossentropy",
            metrics=["accuracy"],
        )

    dataset = train.get_dataset_shard("train")
    results = []

    for epoch in range(epochs):
        print(f"Epoch: {epoch}")
        tf_dataset = dataset.to_tf(
            feature_columns=output_column_name,
            label_columns=label_columns[0],
            batch_size=batch_size
        )
        history = multi_worker_model.fit(
            tf_dataset,
            class_weight=cw,
            callbacks=[ReportCheckpointCallback()]
        )
        results.append(history.history)

    return results


def create_sklearn_standard_scaler(scaler):
    sk_scaler = sklearn.preprocessing.StandardScaler()
    mean = []
    std = []

    for column in feature_columns:
        mean.append(scaler.stats_[f"mean({column})"])
        std.append(scaler.stats_[f"std({column})"])

    sk_scaler.mean_ = np.array(mean)
    sk_scaler.scale_ = np.array(std)
    sk_scaler.var_ = sk_scaler.scale_ ** 2

    return sk_scaler


def save_scalar(scaler):
    s3_resource = get_s3_resource()
    bucket = s3_resource.Bucket(bucket_name)
    sklearn_scaler = create_sklearn_standard_scaler(scaler)

    sk_scaler_filename = "/tmp/scaler.pkl"
    with open(sk_scaler_filename, "wb") as f:
        pickle.dump(sklearn_scaler, f)

    print(f"Uploading scaler from {sk_scaler_filename} to {scaler_output}")
    bucket.upload_file(sk_scaler_filename, scaler_output)


def save_onnx_model(checkpoint_path):
    s3_resource = get_s3_resource()
    bucket = s3_resource.Bucket(bucket_name)

    pipeline_artifacts_s3_resource = get_pipeline_artifacts_s3_resource()
    pipeline_artifacts_bucket = pipeline_artifacts_s3_resource.Bucket(pipeline_artifacts_bucket_name)

    cp_s3_key = checkpoint_path.removeprefix(f"{pipeline_artifacts_bucket_name}/") + "/" + keras_model_filename
    keras_model_local = f"/tmp/{keras_model_filename}"

    print(f"Downloading model state_dict from {cp_s3_key} to {keras_model_local}")
    pipeline_artifacts_bucket.download_file(cp_s3_key, keras_model_local)
    keras_model = tf.keras.models.load_model(keras_model_local)
    onnx_model_local = f"/tmp/model.onnx"
    onnx_model, _ = tf2onnx.convert.from_keras(keras_model)
    onnx.save(onnx_model, onnx_model_local)

    print(f"Uploading model from {onnx_model_local} to {model_output}")
    bucket.upload_file(onnx_model_local, model_output)


pyarrow_fs = get_pyarrow_fs()
pipeline_artifacts_fs = get_pipeline_artifacts_fs()
class_weights = get_class_weights(pyarrow_fs)

config = {"lr": learning_rate, "batch_size": batch_size, "epochs": num_epochs, "class_weight":class_weights}

train_dataset = ray.data.read_csv(
    filesystem=pyarrow_fs,
    paths=f"s3://{bucket_name}/{train_data}")
scaler = StandardScaler(columns=feature_columns)
concatenator = Concatenator(include=feature_columns, output_column_name=output_column_name)
train_dataset = scaler.fit_transform(train_dataset)
train_dataset = concatenator.fit_transform(train_dataset)

print(scaler.stats_)

scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)

trainer = TensorflowTrainer(
    train_loop_per_worker=train_func,
    train_loop_config=config,
    run_config=RunConfig(
        storage_filesystem=pipeline_artifacts_fs,
        storage_path=f"{pipeline_artifacts_bucket_name}/ray/",
        name="fraud-training",
    ),
    scaling_config=scaling_config,
    datasets={"train": train_dataset},
    metadata={"preprocessor_pkl": scaler.serialize()},
)
result = trainer.fit()
metadata = result.checkpoint.get_metadata()
print(metadata)
print(StandardScaler.deserialize(metadata["preprocessor_pkl"]))

save_scalar(scaler)
save_onnx_model(result.checkpoint.path)
