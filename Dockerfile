# Adapted from https://stackoverflow.com/a/72465422

FROM python:3.10

# Configure Poetry
ENV POETRY_VERSION=1.2.0
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VENV=/opt/poetry-venv
ENV POETRY_CACHE_DIR=/opt/.cache

# Install poetry separated from system interpreter
RUN python3 -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install -U pip setuptools \
    && $POETRY_VENV/bin/pip install poetry==${POETRY_VERSION}

# Add `poetry` to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Install dependencies
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml
RUN poetry install -E server

COPY src /app
WORKDIR /app

# Run your app
CMD [ "poetry", "run", "flask", "--app", "isolate.server", "run", "--host", "0.0.0.0", "--port", "5000" ]
