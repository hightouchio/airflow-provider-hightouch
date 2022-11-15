FROM python:3.9

COPY . .

# this project doesnt have a requirements.txt :sigh:
RUN python -m pip install --upgrade pip
RUN pip3 install -e .
RUN pip3 install requests
RUN pip3 install apache-airflow
RUN pip3 install pytest
RUN pip3 install requests_mock

CMD python -m unittest discover

# docker build -t "htairflow:latest" .
# docker run htairflow