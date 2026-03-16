FROM python:3.8-slim

WORKDIR /appPablo

COPY  requiremnts.txt /appPablo

RUN pip install -r requiremnts.txt

COPY ingest.py /appPablo

CMD ["python", ["ingest.py"]]