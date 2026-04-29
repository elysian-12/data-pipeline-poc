FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
 && apt-get install -y --no-install-recommends make build-essential ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --frozen --extra dev --no-install-project || uv sync --extra dev --no-install-project

COPY . .
RUN uv sync --frozen --extra dev || uv sync --extra dev

ENV PATH="/app/.venv/bin:${PATH}"

ENTRYPOINT ["make"]
CMD ["run"]
