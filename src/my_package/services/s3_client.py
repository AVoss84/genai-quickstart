import os
from typing import List, Optional
import json
import io
import logging
import boto3
from botocore.exceptions import ClientError
import traceback
import pandas as pd
from my_package.config import config
from my_package.config import global_config as glob

# Ensure log directory exists
log_directory = os.path.join(glob.UC_CODE_DIR, "logging")
os.makedirs(log_directory, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Client:
    def __init__(self, enable_env: bool = False, verbose: bool = True):
        self.verbose = verbose
        self.config_input, self.config_output = config.io["input"], config.io["output"]
        self.bucket_name = self.config_input["s3"]["bucket"]
        your_aws_profile_name = (
            glob.UC_AWS_PROFILE if glob.UC_AWS_PROFILE != "" else None
        )
        try:
            if enable_env:
                # Use in Fargate:
                session = boto3.Session(profile_name=your_aws_profile_name)

                # # Use in local Docker:
                # session = boto3.Session(
                #     aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                #     aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                #     aws_session_token=os.environ["AWS_SESSION_TOKEN"],
                #     profile_name=your_aws_profile_name,
                # )
            else:
                session = boto3.Session(profile_name=your_aws_profile_name)
            self.s3_client = session.client("s3")

            if self.verbose:
                print("\nS3 client available.")
                print(f"Bucket name: {self.bucket_name}")
        except boto3.exceptions.Boto3Error as e:
            tb = traceback.format_exc()  # get the full traceback
            logger.error(f"An error occurred in S3 client: {e}\n{tb}")
            print(f"An error occurred in S3 client: {e}")
            print("\nNo S3 client available!")

    def get_object(self, bucket: str, key: str) -> object:
        """
        Retrieves an object from the specified S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key of the object to retrieve.

        Returns:
            object: The retrieved object.
        """
        return self.s3_client.get_object(Bucket=bucket, Key=key)

    def put_object(self, bucket: str, key: str, obj: object, **kwargs) -> None:
        """Put an object into S3.

        This method uploads an object to the specified S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key or path of the object in the bucket.
            obj (object): The object to be uploaded.
            **kwargs: Additional arguments to be passed to the `put_object` method.

        Returns:
            None
        """
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=obj, **kwargs)

    def copy_object(
        self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
    ) -> None:
        """Copy an object from one S3 location to another.

        Args:
            source_bucket (str): _description_
            source_key (str): _description_
            dest_bucket (str): _description_
            dest_key (str): _description_
        """
        self.s3_client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=dest_bucket,
            Key=dest_key,
        )

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from S3.

        Args:
            bucket (str): _description_
            key (str): _description_
        """
        self.s3_client.delete_object(Bucket=bucket, Key=key)

    def upload_file(self, bucket: str, key: str, file_path: str) -> None:
        """Upload a file from a local file path to S3.

        Args:
            bucket (str): _description_
            key (str): _description_
            file_path (str): _description_
        """
        self.s3_client.upload_file(Filename=file_path, Bucket=bucket, Key=key)

    def upload_object(self, bucket: str, key: str, obj: object) -> None:
        """
        The file-like object must be in binary mode.
        Example:
        s3_client = S3Client()
        with open('filename', 'rb') as data:
            s3_client.upload_fileobj(data, 'mybucket', 'mykey')
        """
        self.s3_client.upload_fileobj(Fileobj=obj, Bucket=bucket, Key=key)

    def download_file(
        self, key: str, file_path: str, bucket: Optional[str] = None
    ) -> None:
        """Download a file from S3 to a local file path.

        Args:
            key (str): The key of the file in the S3 bucket.
            file_path (str): The local path to save the file.
            bucket (Optional[str]): The name of the S3 bucket. Uses the default bucket if None.
        """
        if bucket is None:
            bucket = self.bucket_name
        try:
            self.s3_client.download_file(Bucket=bucket, Key=key, Filename=file_path)
            if self.verbose:
                print(
                    f"File {key} successfully downloaded from {bucket} to {file_path}"
                )
            logger.info(
                f"File {key} successfully downloaded from {bucket} to {file_path}"
            )
        except ClientError as e:
            logger.error(f"Failed to download {key} from {bucket} to {file_path}: {e}")
            print(e)
            raise e

    def list_keys(self, bucket: str, prefix: str, suffix: str) -> list:
        """
        WARNING By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)[
            "Contents"
        ]
        return [obj["Key"] for obj in response if obj["Key"].endswith(suffix)]

    def key_exists(self, bucket: str, key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            print(e)
            return False

    def load_dict_from_json(self, bucket: str, key: str) -> dict:
        obj = self.get_object(bucket, key)
        return json.load(obj["Body"])

    def put_dict_as_json(self, bucket: str, key: str, dict_file: dict) -> None:
        self.put_object(bucket, key, json.dumps(dict_file))

    def load_df_from_csv(self, bucket: str, key: str, sep: str = ",") -> pd.DataFrame:
        obj = self.get_object(bucket, key)
        return pd.read_csv(obj["Body"], sep=sep)

    def put_df_as_csv(self, bucket: str, key: str, df: pd.DataFrame) -> None:
        self.put_object(bucket, key, df.to_csv(index=False))

    def load_pdf_from_bytes(self, bucket: str, key: str) -> bytes:
        obj = self.get_object(bucket, key)
        return obj["Body"].read()

    def put_bytes_as_pdf(self, bucket: str, key: str, pdf: bytes) -> None:
        self.put_object(bucket, key, pdf)

    def load_img_from_bytes(self, bucket: str, key: str) -> bytes:
        obj = self.get_object(bucket, key)
        return obj["Body"].read()

    def put_bytes_as_img(self, bucket: str, key: str, img: bytes) -> None:
        self.put_object(bucket, key, img)

    def create_bucket(self, bucket: str) -> None:
        self.s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                "LocationConstraint": "eu-central-1",
            },
        )

    def delete_bucket(self, bucket: str) -> None:
        self.s3_client.delete_bucket(Bucket=bucket)

    def load_objects_as_parquet(self, bucket: str, prefix: str) -> pd.DataFrame:
        """Get all parquet objects from a AWS S3 Bucket.
        WARNING By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.
        """
        ls_object_keys = self.list_keys(bucket=bucket, prefix=prefix, suffix=".parquet")
        dfs = []
        for obj_key in ls_object_keys:
            s3_object = self.get_object(bucket=bucket, key=obj_key)
            result = pd.read_parquet(io.BytesIO(s3_object["Body"].read()))
            dfs.append(result)
        df = pd.concat(dfs, ignore_index=True)
        return df

    def load_objects_as_csv(self, bucket: str, prefix: str) -> pd.DataFrame:
        """Get all csv objects from a AWS S3 Bucket.
        WARNING By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.
        """
        ls_object_keys = self.list_keys(bucket=bucket, prefix=prefix, suffix=".csv")
        dfs = []
        for obj_key in ls_object_keys:
            result = self.load_df_from_csv(bucket=bucket, key=obj_key)
            dfs.append(result)
        df = pd.concat(dfs, ignore_index=True)
        return df

    def list_files(
        self,
        bucket_name: Optional[str] = None,
        path: str = "",
        filename_only: bool = True,
    ) -> List[str]:
        """
        List all files from an S3 bucket directory using the client interface.

        Args:
            bucket_name (str): The name of the S3 bucket.
            path (str): S3 bucket directory name.
            filename_only (bool): If True, returns only the filenames without their S3 path.

        Returns:
            List[str]: A list of file names or keys.
        """
        if bucket_name is None:
            bucket_name = self.bucket_name
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=path)

            my_files = []
            for page in page_iterator:
                if "Contents" in page:
                    # Collect all PDF files
                    my_files.extend(
                        [
                            obj["Key"]
                            for obj in page["Contents"]
                            if obj["Key"].lower().endswith(".pdf")
                        ]
                    )

            if filename_only:
                return [
                    os.path.basename(file)
                    for file in my_files
                    if os.path.basename(file)
                ]  # Extract file names only
            else:
                return my_files
        except Exception as ex:
            if self.verbose:
                print(
                    f"Error listing files in bucket {bucket_name} with path '{path}': {ex}"
                )
            return list()
