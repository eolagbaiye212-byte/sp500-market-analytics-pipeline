FROM apache/airflow:3.1.8 
RUN pip install yfinance pandas requests sqlalchemy psycopg2-binary