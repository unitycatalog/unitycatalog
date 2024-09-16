# Quickstart

This quickstart shows how to run Unity Catalog on localhost which is great for experimentation and testing.

## How to start the Unity Catalog server

Start by cloning the open source Unity Catalog GitHub repository:

```
git clone git@github.com:unitycatalog/unitycatalog.git
```

Change into the `unitycatalog` directory and run `bin/start-uc-server` to instantiate the server.  Here is what you should see:

![UC Server](./assets/images/uc_server.png)

Well, that was pretty easy!

To run Unity Catalog, you need Java 17 installed on your machine.  You can always run the `java --version` command to verify that you have the right version of Java installed.

![UC Java Version](./assets/images/uc_java_version.png)

## Verify Unity Catalog server is running

Let’s create a new Terminal window and verify that the Unity Catalog server is running.

Unity Catalog has a few built-in tables that are great for quick experimentation.  Let’s look at all the tables that have a catalog name of “unity” and a schema name of “default” with the Unity Catalog CLI.

```sh
bin/uc table list --catalog unity --schema default
```

![UC list tables](./assets/images/uc_list_tables.png)

Let’s read the content of the `unity.default.numbers` table with the Unity Catalog CLI.
```sh
bin/uc table read --full_name unity.default.numbers
```

![UC query table](./assets/images/uc_query_table.png)

We can see it’s straightforward to make queries with the Unity Catalog CLI.

## Unity Catalog structure

Unity Catalog stores all assets in a 3-level namespace:

1. catalog
2. schema
3. assets like tables, volumes, functions, etc.

![UC 3 Level](./assets/images/uc-3-level.png)

Here's an example Unity Catalog instance:

![UC Example Catalog](./assets/images/uc_example_catalog.png)

This Unity Catalog instance contains a single catalog named `cool_stuff`.

The `cool_stuff` catalog contains two schema: `thing_a` and `thing_b`.

`thing_a` contains a Delta table, a function, and a Lance volume.  `thing_b` contains two Delta tables.

Unity Catalog provides a nice organizational structure for various datasets.

## List the catalogs and schemas with the CLI

The UC server is pre-populated with a few sample catalogs, schemas, Delta tables, etc.

Let's start by listing the catalogs using the CLI.

```sh
bin/uc catalog list
```

You should see a catalog named `unity`. Let's see what's in this `unity` catalog (pun intended).

```sh
bin/uc schema list --catalog unity
```

![UC list schemas](./assets/images/uc_quickstart_schema_list.png)

You should see that there is a schema named `default`.  To go deeper into the contents of this schema,
you have to list different asset types separately. Let's start with tables.

## Operate on Delta tables with the CLI

Let's list the tables.

```sh
bin/uc table list --catalog unity --schema default
```

![UC list tables](./assets/images/uc_quickstart_table_list.png)

You should see a few tables. Some details are truncated because of the nested nature of the data.
To see all the content, you can add `--output jsonPretty` to any command.

Next, let's get the metadata of one those tables.

```sh
bin/uc table get --full_name unity.default.numbers
```

![UC table metadata](./assets/images/uc_quickstart_table_metadata.png)

You can see this is a Delta table from the `DATA_SOURCE_FORMAT` metadata.

Here's how to print a snippet of a Delta table (powered by the [Delta Kernel Java](https://delta.io/blog/delta-kernel/) project).

```sh
bin/uc table read --full_name unity.default.numbers
```

![UC table contents](./assets/images/uc_quickstart_table_contents.png)

Let's try creating a new table.

```sh
bin/uc table create --full_name unity.default.my_table \
--columns "col1 int, col2 double" --storage_location /tmp/uc/my_table
```

![UC create table](./assets/images/uc_create_table.png)

If you list the tables again, you should see this new table.

Next, append some randomly generated data to the table.

```sh
bin/uc table write --full_name unity.default.my_table
```

Read the table to confirm the random data was appended:

```sh
bin/uc table read --full_name unity.default.my_table
```

![UC read random table](./assets/images/uc_read_random_table.png)

Delete the table to clean up:

```sh
bin/uc table delete --full_name unity.default.my_table
```

## Manage models in Unity Catalog using MLflow

Unity Catalog supports the management and governance of ML models as securable assets.  Starting with 
[MLflow 2.16.1](https://mlflow.org/releases/2.16.1), MLflow offers integrated support for using Unity Catalog as the 
backing resource for the MLflow model registry.  What this means is that with the MLflow client, you will be able to 
interact directly with your Unity Catalog service for the creation and access of registered models.

## Setup MLflow for usage with Unity Catalog

In your desired development environment, install MLflow 2.16.1 or higher:

```sh
$ pip install mlflow
```

The installation of MLflow includes the MLflow CLI tool, so you can start a local MLflow server with UI by running the command below in your terminal:

```sh
$ mlflow ui
```

It will generate logs with the IP address, for example:

```
(mlflow) [master][~/Documents/mlflow_team/mlflow]$ mlflow ui
[2023-10-25 19:39:12 -0700] [50239] [INFO] Starting gunicorn 20.1.0
[2023-10-25 19:39:12 -0700] [50239] [INFO] Listening at: http://127.0.0.1:5000 (50239)
```

Next, from within a python script or shell, import MLflow and set the tracking URI and the registry URI.

```python
import mlflow

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_registry_uri("uc:http://127.0.0.1:8080")
```

At this point, your MLflow environment is ready for use with the newly started MLflow tracking server and the Unity Catalog server acting as your model registry.

You can quickly train a test model and validate that the MLflow/Unity catalog integration is fully working.

```python
import os
from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pandas as pd

X, y = datasets.load_iris(return_X_y=True, as_frame=True)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

with mlflow.start_run():
    # Train a sklearn model on the iris dataset
    clf = RandomForestClassifier(max_depth=7)
    clf.fit(X_train, y_train)
    # Take the first row of the training dataset as the model input example.
    input_example = X_train.iloc[[0]]
    # Log the model and register it as a new version in UC.
    mlflow.sklearn.log_model(
        sk_model=clf,
        artifact_path="model",
        # The signature is automatically inferred from the input example and its predicted output.
        input_example=input_example,
        registered_model_name="unity.default.iris",
    )

loaded_model = mlflow.pyfunc.load_model(f"models:/unity.default.iris/1")
predictions = loaded_model.predict(X_test)
iris_feature_names = datasets.load_iris().feature_names
result = pd.DataFrame(X_test, columns=iris_feature_names)
result["actual_class"] = y_test
result["predicted_class"] = predictions
result[:4]
```

This code snippet will create a registered model `default.unity.iris` and log the trained model as model version 1.  It then loads the model from the Unity Catalog server, and performs batch inference on the test set using the loaded model.

## APIs and Compatibility

- Open API specification: See the Unity Catalog Rest API.
- Compatibility and stability: The APIs are currently evolving and should not be assumed to be stable.
