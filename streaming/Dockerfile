FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install only the producer's runtime dependencies
RUN pip install --no-cache-dir \
    requests==2.32.3 \
    tenacity==9.0.0 \
    pydantic==2.8.2 \
    python-dotenv==1.0.1 \
    azure-eventhub==5.12.2 \
    azure-identity==1.17.1

COPY streaming/ ./streaming/
COPY ingestion/contracts/ ./ingestion/contracts/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import streaming.producer; print('ok')" || exit 1

CMD ["python", "-m", "streaming.producer"]
