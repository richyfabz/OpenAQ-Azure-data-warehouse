# Runtime image for the OpenAQ ingestion CLI (the unit Airflow invokes in Phase 5).
FROM python:3.11-slim

WORKDIR /app

# Dependencies first: this layer caches until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 300 --retries 10 -r requirements.txt

# Ship the application only — not the repo.
COPY config/ config/
COPY ingestion/ ingestion/

RUN useradd --create-home app
USER app

ENTRYPOINT ["python", "-m", "ingestion.pipeline"]
CMD ["--help"]
