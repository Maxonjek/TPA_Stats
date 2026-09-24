FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libmariadb-dev \
        iputils-ping \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY BD/requirements.txt /app/requirements.txt
COPY requirements.txt /app/requirements_main.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && pip install --no-cache-dir -r /app/requirements_main.txt

COPY main.py /app/main.py
COPY BD /app/BD

EXPOSE 8000

CMD ["python", "main.py"]