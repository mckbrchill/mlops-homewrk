FROM python:3.10.14-slim

ENV POETRY_VERSION=1.4.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN which java
RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

WORKDIR /app

COPY pyproject.toml /app/
RUN poetry install --only main --no-interaction

COPY . /app/

CMD ["poetry", "run", "python", "app/main.py"]