# syntax=docker/dockerfile:1.7-labs
# Necessary to support the COPY --exclude option

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base

# create the local user based on the current user
ARG USER_ID=1000
ENV GROUP_ID=1000

ENV USERNAME=appuser

RUN groupadd -g ${GROUP_ID} ${USERNAME} \
    && useradd -m -u ${USER_ID} -g ${USERNAME} ${USERNAME}


# Set environment variables to prevent Python from writing .pyc files and buffering output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set a working directory inside the container
WORKDIR /app/

# Copy the application code into the container
COPY agent_companion/agent_companion/__init__.py /app/agent_companion/agent_companion/
COPY agent_companion/pyproject.toml /app/agent_companion/
COPY agent_companion/uv.lock /app/agent_companion/

WORKDIR /app/agent_companion

# Ensure the group can read, write, and execute files in /app
RUN chown -R ${USER_ID}:${GROUP_ID} /app
RUN chmod 775 /app

# Switch to the created user
USER ${USERNAME}

# Configure UV
ENV UV_COMPILE_BYTECODE=1
ENV UV_SYSTEM_PYTHON=1

ENV VIRTUAL_ENV=/app/.venv
ENV UV_PYTHON_ENV=${VIRTUAL_ENV}
ENV UV_PROJECT_ENVIRONMENT=${VIRTUAL_ENV}

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Place executables in the environment at the front of the path
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"
ENV PYTHONPATH=/app/agent_companion:/app/common

ENTRYPOINT []


###################
# Dev build target
###################
FROM base AS dev

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen


# USER root
# RUN apt-get update \
#     && apt-get install -y postgresql-client

USER ${USERNAME}


###################
# Prod build target
###################
FROM base AS release

COPY --exclude=**/*test.py --exclude=**/.venv agent_companion /app/agent_companion/
COPY --exclude=**/*test.py --exclude=**/.venv common /app/common/ 

CMD ["uvicorn", "agent_companion.main:app", \
    "--host", "0.0.0.0", \
    "--port", "5602" \
    ]
