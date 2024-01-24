## ----------------------------------------------------------------------
## Welcome to the project's Makefile
## ----------------------------------------------------------------------

help: ## show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

build: ## build the solution
	docker compose build --no-cache

run: ## run the solution
	echo "Running Airflow locally using the LocalExecutor"
	docker compose up -d

stop: ## stop running every container
	echo "Stopping all containers"
	docker-compose -f docker-compose.yml down -v --remove-orphans

enter-warehouse: ## enter the postgres DB with SQL
	docker exec -it coderhouse-final-project-demo_data_warehouse_1 psql -U airflow_dw --dbname dw

bash: ## Enter the airflow container with bash
	docker exec webserver bash

# Reinstall the requirements.txt file and then resets
make install: 
	make build
	make run

# Resets the containers
reset:
	make stop
	make run