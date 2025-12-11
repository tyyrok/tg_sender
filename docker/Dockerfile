FROM python:3.12-slim

ENV APP_USER=appuser \
    APP_GROUP=appgroup \
    VENV_PATH="/app/.venv"
RUN apt-get update && apt-get install -y \
    gcc g++ libffi-dev libssl-dev python3-dev curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir uv

RUN groupadd --system "$APP_GROUP" && useradd -m --system "$APP_USER" --groups "$APP_GROUP"

# Рабочая директория
WORKDIR /app

COPY . .

RUN chown -R "$APP_USER":"$APP_GROUP" /app

USER $APP_USER

RUN uv venv "$VENV_PATH"
ENV PATH="$VENV_PATH/bin:$PATH"
ENV PYTHONPATH=/app/src

RUN uv lock && uv sync --no-dev
