FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app ./app

# Run as a non-root user
RUN useradd --create-home --uid 10001 appuser
USER 10001

EXPOSE 8000 8001

# Override CMD with "app.consumer" to run the consumer
ENTRYPOINT ["python", "-m"]
CMD ["app.producer"]
