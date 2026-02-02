FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

# Wait for Postgres socket before starting Streamlit
CMD sh -c "until nc -z $PGHOST $PGPORT; do echo '[dashboard] waiting for postgres...'; sleep 2; done; streamlit run Dashboard.py --server.port=8501 --server.address=0.0.0.0"