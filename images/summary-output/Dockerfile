FROM python:3.12

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY summary_output.py ./
COPY ppl_per_country.sql ./
