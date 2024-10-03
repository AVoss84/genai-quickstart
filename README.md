# GenAI quick start - Entity extraction tutorial

This little tutorial focuses on the task of entity extraction from a PDF file using OCR + GenAI. The use of LangChain is illustrated as a convenient tool library.

## Package structure

```
├── CHANGELOG.md
├── Dockerfile_Fastapi
├── Dockerfile_Streamlit
├── README.md
├── TODOS.md
├── build_image.sh
├── build_push_ecr.sh
├── docker-compose.yaml
├── logs
├── main.py
├── requirements.txt
├── setup.py
├── src
│   ├── my_package
│   │   ├── config
│   │   │   ├── config.py
│   │   │   ├── global_config.py
│   │   │   └── input_output.yaml
│   │   ├── resources
│   │   │   ├── README.md
│   │   │   ├── postprocessor.py
│   │   │   ├── predictor.py
│   │   │   ├── preprocessor.py
│   │   │   └── trainer.py
│   │   ├── services
│   │   │   ├── README.md
│   │   │   ├── file.py
│   │   │   ├── file_aws.py
│   │   │   ├── pipelines.py
│   │   │   └── publisher.py
│   │   └── utils
│   └── notebooks
└── streamlit_app.py
```

## Use Case description

**Business goal**: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 

**Business stakeholders**: Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.

**Input data description**: Iris data set

**Business impact KPI**: Faster STP (in hours/days)


## Package installation and application develoment

Create conda virtual environment with required packages 
```bash
python3 -m venv quick_start
source quick_start/bin/activate
# optional
conda create -n quick_start python=3.12 -y
conda activate quick_start
```

To install the package locally execute the following steps:

```bash
pip install -r requirements.txt         
pip install -e .               # install my_package in editable mode
```
