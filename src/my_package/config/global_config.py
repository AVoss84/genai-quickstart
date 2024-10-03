import os
from pathlib import Path

# -------------------------------
# Which environment to use?
# -------------------------------
using = "vm"  # local machine
# using = "docker"  # container
# -------------------------------


## Check if required environment variables exist
## if not apply default paths from test environment:
# -----------------------------------------------------------
if using == "vm":
    defaults = {
       "UC_CODE_DIR": str(Path.cwd() / "src"),
        "UC_DATA_DIR": "",  # external
        "UC_DATA_PKG_DIR": str(Path.cwd() / "src" / "data"),
        "UC_PORT": "5000",
        "UC_PORT_POSTGRES": "5432",
        "UC_APP_CONNECTION": "127.0.0.1",
        "UC_AWS_PROFILE": "default",
        "UC_AWS_ENV": "False",
    }
else:
    defaults = {
        "UC_CODE_DIR": "/app/src/",
        "UC_DATA_DIR": "/app/data/",
        "UC_DATA_PKG_DIR": "/app/src/data/",  # data folder within package
        "UC_PORT": "80",
        "UC_PORT_POSTGRES": "5432",
        "UC_APP_CONNECTION": "0.0.0.0",
        "UC_AWS_PROFILE": "",
        "UC_AWS_ENV": "True",
    }
# -------------------------------------------------------------------------------------------------------------------------------

for env in defaults.keys():
    if env not in os.environ:
        os.environ[env] = defaults[env]
        print(
            f"Environment Variable: {str(env)} has been set to default: {str(os.environ[env])}"
        )

UC_CODE_DIR = os.environ["UC_CODE_DIR"]
UC_DATA_DIR = os.environ["UC_DATA_DIR"]
UC_DATA_PKG_DIR = os.environ["UC_DATA_PKG_DIR"]
UC_AWS_ENV = os.environ[
    "UC_AWS_ENV"
]  # should AWS_*** environment variables be used or taken from credentials config?
UC_AWS_PROFILE = os.environ["UC_AWS_PROFILE"]  # AWS profile to use
UC_PORT = os.environ["UC_PORT"]
UC_APP_CONNECTION = os.environ["UC_APP_CONNECTION"]
UC_PORT_POSTGRES = os.environ["UC_PORT_POSTGRES"]
