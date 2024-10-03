from typing import Optional, Callable
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


def s3_health_check(s3_client: Optional[Callable] = None):
    """Check if AWS S3 credentials are still valid by attempting to list buckets.
    Args:
        s3_client (Optional[Callable], optional): _description_. Defaults to None.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    try:
        # Attempt to list buckets as a health check
        s3_client.list_buckets()
        print("S3 credentials are valid \U0001F600")
    except NoCredentialsError:
        print("Credentials not available. \U0001F622")
    except PartialCredentialsError:
        print("Incomplete credentials. \U0001F622")
    except ClientError as e:
        # Handle other possible errors, such as permissions issues
        if e.response["Error"]["Code"] == "InvalidClientTokenId":
            print("Invalid credentials. \U0001F622‚")
        else:
            # Log other ClientError exceptions without assuming credentials are invalid
            print(f"Error accessing S3: {e}")
    except Exception as e:
        # Catch-all for any other exceptions
        print(f"An unexpected error occurred: {e}")