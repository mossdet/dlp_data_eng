FROM python:3.11.11

RUN apt-get install wget
RUN pip install numpy pandas pyarrow sqlalchemy psycopg2

# do a cd to this directory
WORKDIR /app

#COPY source dest
COPY ingest_data.py ingest_data.py

#ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "ingest_data.py" ]
