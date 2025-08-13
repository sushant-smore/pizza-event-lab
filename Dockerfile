# Dockerfile
FROM python:3.9-slim-buster
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY webapp.py .
COPY processor.py .
EXPOSE 8000
ENV KAFKA_BOOTSTRAP_SERVERS="192.168.12.82:9092"
ENV KAFKA_SECURITY_PROTOCOL="PLAINTEXT"
ENV EDB_HOST="192.168.12.28"
ENV EDB_PORT="5444"
ENV EDB_DATABASE="pizza"
ENV EDB_USER="enterprisedb"
ENV EDB_PASSWORD="admin@123"
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
CMD ["./entrypoint.sh"]
