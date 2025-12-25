.PHONY: migrate extract build up down logs clean quick-setup

# Full migration process
migrate: extract build up

# Extract data from VM (if possible)
extract:
	@echo " Starting data extraction..."
	@bash vm_to_docker_migration.sh extract

# Build Docker images
build:
	docker compose build

# Start the environment
up:
	docker compose up -d
	@echo " Environment started!"
	@echo " Airflow: http://localhost:8080"
	@echo " Jupyter: http://localhost:8888"

# Stop everything
down:
	docker compose down

# View logs
logs:
	docker compose logs -f

# Clean everything
clean:
	docker compose down -v
	docker system prune -f

# Quick setup without migration
quick-setup:
	@echo "⚡ Quick setup without VM data..."
	@touch retaildb_backup.sql
	@mkdir -p extracted/airflow/dags extracted/github
	@$(MAKE) build up
