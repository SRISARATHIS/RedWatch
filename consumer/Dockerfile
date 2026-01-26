FROM python:3.11-slim

WORKDIR /app

# Install Python deps first (better docker layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy consumer code
COPY app/ /app/

# Default: run the Snowflake sink consumer.
# Override in docker-compose with COMMAND if you want consumer_base.py instead.
CMD ["python", "consumer_to_postgres.py"]