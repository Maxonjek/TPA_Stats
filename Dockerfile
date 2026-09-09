FROM python:3.12-slim

# Не создавать .pyc и не буферизовать stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Системные зависимости
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libmariadb-dev \
        iputils-ping \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Сначала requirements для кэширования Docker layer
COPY BD/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Копируем приложение
COPY main.py /app/main.py
COPY BD /app/BD

EXPOSE 8000

CMD ["python", "main.py"]