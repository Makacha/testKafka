FROM python:3.7.10

WORKDIR ./

COPY ./requirements.txt ./

RUN pip install -r requirements.txt

COPY ./ ./

CMD ["python", "producer.py"]

