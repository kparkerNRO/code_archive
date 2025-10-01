######################################################
# Common Makefile for all services
# This Makefile is used to define common tasks for all services in the project.
# It expects the following environment variables to be set:
# - SERVICE_NAME: The name of the service (e.g., data_pipeline, agent_companion).
######################################################

export UID = $(shell id -u)

ROOT_DIR = "../.."
DOCKER_COMMAND = docker compose -f ${ROOT_DIR}/dev/docker-compose.yml
DOCKER_RUN_COMMAND = ${DOCKER_COMMAND} run --rm  --entrypoint "bash -c" ${SERVICE_NAME}
PYTEST_RUN_COMMAND = ${DOCKER_COMMAND} run --rm  --entrypoint ""  -w /app ${SERVICE_NAME}

# ---------------------------------------------------------------------------- #
# lifecycle commands
# ---------------------------------------------------------------------------- #
build: ## build the service
	${DOCKER_COMMAND} build ${SERVICE_NAME}

exec: ## Start a bash session in the service container
	${DOCKER_RUN_COMMAND} /bin/bash


# ---------------------------------------------------------------------------- #
# Format & Testing
# ---------------------------------------------------------------------------- #
format: ## format the code
	${DOCKER_RUN_COMMAND} "ruff format /app && ruff check --fix /app"

mypy: ## run mypy type checker
	${DOCKER_RUN_COMMAND} "mypy /app/common /app/${SERVICE_NAME}"

test: ## run all tests
	${PYTEST_RUN_COMMAND} pytest -c ${SERVICE_NAME}/pyproject.toml

run-test: # run a specific test, e.g. make run-test test_pattern='test_foo'
	${PYTEST_RUN_COMMAND} pytest -c ${SERVICE_NAME}/pyproject.toml -k "${test_pattern}"

check-ci: ## run all checks for CI
	${DOCKER_RUN_COMMAND} "ruff check \
		&& \
		mypy /app/common /app/${SERVICE_NAME} \
		&& \
		pytest -c ${SERVICE_NAME}/pyproject.toml"

# ---------------------------------------------------------------------------- #
# Dependency Management helper commands
# ---------------------------------------------------------------------------- #
lock: ## lock dependencies
	${DOCKER_RUN_COMMAND} "uv lock"