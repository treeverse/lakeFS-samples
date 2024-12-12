import os

from kfp import compiler
from kfp import dsl
from kfp.dsl import InputPath, OutputPath

from kfp import kubernetes


@dsl.component(base_image="quay.io/modh/runtime-images:runtime-cuda-tensorflow-ubi9-python-3.9-2024a-20240523")
def get_data(train_data_output_path: OutputPath(), validate_data_output_path: OutputPath()):
    import urllib.request
    print("starting download...")
    print("downloading training data")
    url = "https://raw.githubusercontent.com/cfchase/fraud-detection/main/data/train.csv"
    urllib.request.urlretrieve(url, train_data_output_path)
    print("train data downloaded")
    print("downloading validation data")
    url = "https://raw.githubusercontent.com/cfchase/fraud-detection/main/data/validate.csv"
    urllib.request.urlretrieve(url, validate_data_output_path)
    print("validation data downloaded")


@dsl.component(
    base_image="quay.io/modh/runtime-images:runtime-cuda-tensorflow-ubi9-python-3.9-2024a-20240523",
    packages_to_install=["onnx", "onnxruntime", "tf2onnx", "lakefs==0.7.1", "s3fs==2024.10.0"],
)
def train_model(train_data_input_path: InputPath(), validate_data_input_path: InputPath(), model_output_path: OutputPath()):
    import numpy as np
    import pandas as pd
    from keras.models import Sequential
    from keras.layers import Dense, Dropout, BatchNormalization, Activation
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.utils import class_weight
    import tf2onnx
    import onnx
    import pickle
    from pathlib import Path
    import lakefs
    import os
    import s3fs

    # Define lakeFS Storage and Repository information
    lakefs_storage_options={
          "key": os.environ.get('LAKECTL_CREDENTIALS_ACCESS_KEY_ID'),
          "secret": os.environ.get('LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY'),
          "client_kwargs": {
             "endpoint_url": os.environ.get('LAKECTL_SERVER_ENDPOINT_URL')
          }
    }
    
    repo_name = os.environ.get('LAKEFS_REPO_NAME')
    mainBranch = "main"
    trainingBranch = "train01"

    repo = lakefs.Repository(repo_name)
    print(repo)

    # Create Training branch in lakeFS and load the CSV data to the training branch in lakeFS
    branchTraining = repo.branch(trainingBranch).create(source_reference=mainBranch, exist_ok=True)
    
    obj = branchTraining.object(path='data/train.csv')
    with open(train_data_input_path, mode='rb') as reader, obj.writer(mode='wb', metadata={'using': 'python_wrapper', 'source':'Fraud Detection Demo'}) as writer:
        writer.write(reader.read())
    
    obj = branchTraining.object(path='data/validate.csv')
    with open(validate_data_input_path, mode='rb') as reader, obj.writer(mode='wb', metadata={'using': 'python_wrapper', 'source':'Fraud Detection Demo'}) as writer:
        writer.write(reader.read())
    
    # Load the CSV data which we will use to train the model.
    # It contains the following fields:
    #   distancefromhome - The distance from home where the transaction happened.
    #   distancefromlast_transaction - The distance from last transaction happened.
    #   ratiotomedianpurchaseprice - Ratio of purchased price compared to median purchase price.
    #   repeat_retailer - If it's from a retailer that already has been purchased from before.
    #   used_chip - If the (credit card) chip was used.
    #   usedpinnumber - If the PIN number was used.
    #   online_order - If it was an online order.
    #   fraud - If the transaction is fraudulent.


    feature_indexes = [
        1,  # distance_from_last_transaction
        2,  # ratio_to_median_purchase_price
        4,  # used_chip
        5,  # used_pin_number
        6,  # online_order
    ]

    label_indexes = [
        7  # fraud
    ]

    X_train = pd.read_csv(f"s3://{repo_name}/{trainingBranch}/data/train.csv", storage_options=lakefs_storage_options)
    y_train = X_train.iloc[:, label_indexes]
    X_train = X_train.iloc[:, feature_indexes]

    X_val = pd.read_csv(f"s3://{repo_name}/{trainingBranch}/data/validate.csv", storage_options=lakefs_storage_options)
    y_val = X_val.iloc[:, label_indexes]
    X_val = X_val.iloc[:, feature_indexes]

    # Scale the data to remove mean and have unit variance. The data will be between -1 and 1, which makes it a lot easier for the model to learn than random (and potentially large) values.
    # It is important to only fit the scaler to the training data, otherwise you are leaking information about the global distribution of variables (which is influenced by the test set) into the training set.

    scaler = StandardScaler()

    X_train = scaler.fit_transform(X_train.values)

    obj = branchTraining.object(path='artifact/scaler.pkl')
    with obj.writer("wb") as handle:
        pickle.dump(scaler, handle)

    # Since the dataset is unbalanced (it has many more non-fraud transactions than fraudulent ones), set a class weight to weight the few fraudulent transactions higher than the many non-fraud transactions.
    class_weights = class_weight.compute_class_weight('balanced', classes=np.unique(y_train), y=y_train.values.ravel())
    class_weights = {i: class_weights[i] for i in range(len(class_weights))}

    # Build the model, the model we build here is a simple fully connected deep neural network, containing 3 hidden layers and one output layer.

    model = Sequential()
    model.add(Dense(32, activation='relu', input_dim=len(feature_indexes)))
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
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    model.summary()

    # Train the model and get performance

    epochs = 2
    history = model.fit(X_train, y_train, epochs=epochs,
                        validation_data=(scaler.transform(X_val.values), y_val),
                        verbose=True, class_weight=class_weights)

    # Save the model as ONNX for easy use of ModelMesh
    model_proto, _ = tf2onnx.convert.from_keras(model)
    print(model_output_path)
    onnx.save(model_proto, model_output_path)


@dsl.component(
    base_image="quay.io/modh/runtime-images:runtime-cuda-tensorflow-ubi9-python-3.9-2024a-20240523",
    packages_to_install=["boto3", "botocore", "lakefs==0.7.1"]
)
def upload_model(input_model_path: InputPath()):
    import os
    import boto3
    import botocore

    # Define lakeFS Repository
    import lakefs
    repo_name = os.environ.get('LAKEFS_REPO_NAME')
    
    mainBranch = "main"
    trainingBranch = "train01"
    
    repo = lakefs.Repository(repo_name)
    print(repo)

    aws_access_key_id = os.environ.get('LAKECTL_CREDENTIALS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY')
    endpoint_url = os.environ.get('LAKECTL_SERVER_ENDPOINT_URL')
    region_name = os.environ.get('LAKEFS_DEFAULT_REGION')
    bucket_name = os.environ.get('LAKEFS_REPO_NAME')

    s3_key = os.environ.get("S3_KEY")

    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)

    s3_resource = session.resource(
        's3',
        config=botocore.client.Config(signature_version='s3v4'),
        endpoint_url=endpoint_url,
        region_name=region_name)

    bucket = s3_resource.Bucket(bucket_name)

    print(f"Uploading {trainingBranch}/{s3_key}")
    bucket.upload_file(input_model_path, f"{trainingBranch}/{s3_key}")


@dsl.pipeline(name=os.path.basename(__file__).replace('.py', ''))
def pipeline():
    get_data_task = get_data()
    train_data_csv_file = get_data_task.outputs["train_data_output_path"]
    validate_data_csv_file = get_data_task.outputs["validate_data_output_path"]

    train_model_task = train_model(train_data_input_path=train_data_csv_file,
                                   validate_data_input_path=validate_data_csv_file)
    onnx_file = train_model_task.outputs["model_output_path"]

    upload_model_task = upload_model(input_model_path=onnx_file)

    upload_model_task.set_env_variable(name="S3_KEY", value="models/fraud/1/model.onnx")

    kubernetes.use_secret_as_env(
        task=train_model_task,
        secret_name='my-storage',
        secret_key_to_env={
            'AWS_ACCESS_KEY_ID': 'LAKECTL_CREDENTIALS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY': 'LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',
            'AWS_DEFAULT_REGION': 'LAKEFS_DEFAULT_REGION',
            'AWS_S3_BUCKET': 'LAKEFS_REPO_NAME',
            'AWS_S3_ENDPOINT': 'LAKECTL_SERVER_ENDPOINT_URL',
        })

    kubernetes.use_secret_as_env(
        task=upload_model_task,
        secret_name='my-storage',
        secret_key_to_env={
            'AWS_ACCESS_KEY_ID': 'LAKECTL_CREDENTIALS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY': 'LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',
            'AWS_DEFAULT_REGION': 'LAKEFS_DEFAULT_REGION',
            'AWS_S3_BUCKET': 'LAKEFS_REPO_NAME',
            'AWS_S3_ENDPOINT': 'LAKECTL_SERVER_ENDPOINT_URL',
        })


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=__file__.replace('.py', '.yaml')
    )
