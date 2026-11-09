FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY maritime ./maritime
RUN pip install .

COPY web ./web
ENV WEB_DIR=/app/web PORTS_CACHE=/app/data/ports.json

# La commande est définie par service dans docker-compose.yml
CMD ["python", "-m", "maritime.producer"]
