FROM python:3.12

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY mysql_schema.py ./

# Autorun
CMD ["python", "mysql_schema.py"]