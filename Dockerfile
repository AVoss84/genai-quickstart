#FROM python:3.8
FROM registry.regional.devops-services.ec1.aws.aztec.cloud.allianz/dockerhub/library/python:3.12.2
RUN apt-get clean -y && apt-get update -y && apt-get install bash
RUN pip install --upgrade pip 
EXPOSE 5000
COPY ./main.py /app/main.py
COPY ./streamlit_app.py /app/streamlit_app.py
COPY ./requirements.txt /app/requirements.txt
COPY ./src /app/src
COPY ./.streamlit /app/.streamlit
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]


