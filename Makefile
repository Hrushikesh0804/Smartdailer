.PHONY: up down init-db test

# Start Postgres in the background and wait until healthy.
up:
	docker-compose up -d --wait

# Stop and remove containers (keeps the volume by default).
down:
	docker-compose down

# Drop and recreate schema on both databases.
init-db:
	python -m smartdialer.db.init_db

# Run the full test suite against smartdialer_test.
test:
	pytest
