FROM python:3.11-slim

WORKDIR /usr/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml .
COPY profiles.yml .
COPY models ./models

# Run dbt each time container starts (simple)
CMD ["dbt", "run", "--profiles-dir", ".", "--project-dir", "."]
