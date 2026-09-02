FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home --uid 10001 etl \
    && mkdir /state \
    && chown etl:etl /state

COPY --chown=etl:etl replicate.py .

USER etl

CMD ["python", "replicate.py"]

