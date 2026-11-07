# Runtime image for the OpenAQ ingestion CLI (the unit Airflow invokes in Phase 5).
FROM python:3.11-slim

ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=15

WORKDIR /app

RUN useradd --create-home app

# Dependencies first: this layer caches until the ingestion requirements change.
COPY requirements-ingestion.txt .
RUN pip install --no-cache-dir -r requirements-ingestion.txt

# Ship the application only — not the repo.
COPY config/ config/
COPY ingestion/ ingestion/

RUN chown -R app:app /app
USER app

ENTRYPOINT ["python", "-m", "ingestion.pipeline"]
CMD ["--help"]
