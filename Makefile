setup:
	cp .env.example .env 
	mkdir -p airflow/logs airflow/config airflow/plugins data/lake data/logs
	mkdir -p data/warehouse
up:
	docker compose up
down:
	docker compose down

