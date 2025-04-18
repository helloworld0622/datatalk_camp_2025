FROM python:3.12.2

RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]
