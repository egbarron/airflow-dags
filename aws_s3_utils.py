from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = "class_datasets_aws_bucket"
BUCKET_NAME = "classdatasets"
SUBFOLDER = "egb123_converted_xlsx"
TEST_FILE = NamedTemporaryFile(delete=False, delete_on_close=False)


def list_my_objects():
    # Code to list objects in the subfolder
    # -------------------------------------------------------
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = SUBFOLDER + "/"
    objects = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    if objects:
        print(f"Objects in {prefix}: {objects}")
    else:
        print(f"No objects found in {prefix} or subfolder does not exist.")


def upload_object_to_my_folder():
    # Code to upload an object to the subfolder
    # -------------------------------------------------------
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    key = f"{SUBFOLDER}/{TEST_FILE.name}"
    hook.load_file(filename=TEST_FILE.name, key=key, bucket_name=BUCKET_NAME)
    print(f"Uploaded {TEST_FILE.name} to {key}")


def delete_my_folder():
    # Code to delete a subfolder and it contents
    # -------------------------------------------------------
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = SUBFOLDER + "/"
    objects = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)
    if objects:
        for obj in objects:
            hook.delete_objects(bucket=BUCKET_NAME, keys=obj)
        print(f"Deleted all objects in {prefix}")
    else:
        print(f"No objects found in {prefix} or subfolder does not exist.")


with DAG("aws_s3_utils", schedule=None) as dag:
    list_my_objects_task = PythonOperator(
        task_id="list_my_objects", python_callable=list_my_objects
    )

    upload_object_to_my_folder_task = PythonOperator(
        task_id="upload_object_to_my_folder",
        python_callable=upload_object_to_my_folder,
    )

    delete_my_folder_task = PythonOperator(
        task_id="delete_my_folder", python_callable=delete_my_folder
    )

    # Bash Operator to create a file that will be uploaded
    # -------------------------------------------------------
    create_file_task = BashOperator(
        task_id="create_file_task",
        bash_command=f"echo 'Hello World!' > {TEST_FILE.name}",
    )
