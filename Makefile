#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = de-project
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## install all python packages and dependencies
requirements:
	$(call pip install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install flake8
flake:
	$(call execute_in_env, $(PIP) install flake8)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Install pytest
pytest:
	$(call execute_in_env, $(PIP) install pytest)

## Install autopep8
autopep:
	$(call execute_in_env, $(PIP) install autopep8)

## Set up dev requirements (bandit, safety, flake8, coverage, autopep and pytest)
dev-setup: bandit safety flake coverage pytest, autopep

# Build / Run
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the flake8 code check
run-flake:
	$(call execute_in_env, flake8 \
	./src/multiplication_table/*.py \
	./src/reduce_by_steps/*.py \
	./src/find_partner/*.py \
	./src/find_most_repeated/*.py \
	./src/vowel_shift/*.py \
	./src/alternating_split/*.py \
	./src/crack_code/*.py \
	./src/binary_search/*.py \
	./src/calculate_binary_score/*.py \
	./src/justify_line/*.py \
	./src/find_the_needle/*.py \
	./src/validate_suduko/*.py \
	./src/strange_sort/*.py \
	./src/gdpr_mask/*.py )

## Run autopep8 code formatting
run-autopep8:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} autopep8 -a ${file_name})


## Run a single test
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v ${test_run})

## Run all the unit tests
unit-tests:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vrP -s)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest && coverage report -m)

## Run all checks
run-checks: run-flake unit-tests 