#!/bin/bash
set -e

echo "🔄 Starting Kiwilytics Docker Environment..."

# Wait for PostgreSQL
echo "⏳ Waiting for PostgreSQL..."
while ! pg_isready -h postgres -p 5432 -U kiwilytics; do
    sleep 1
done
echo " PostgreSQL is ready!"

# Update Airflow database
airflow db upgrade

# Start services
echo " Starting Airflow webserver..."
airflow webserver --port 8080 &

echo " Starting Airflow scheduler..."
airflow scheduler &

echo "📓 Starting Jupyter notebook..."
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root \
    --NotebookApp.token='' --NotebookApp.password='' \
    --notebook-dir=/home/kiwilytics &

echo " All services started!"
echo " Access points:"
echo "   - Airflow: http://localhost:8080 (kiwilytics/kiwilytics)"
echo "   - Jupyter: http://localhost:8888"
echo "   - PostgreSQL: localhost:5432 (kiwilytics/kiwilytics)"

# Keep container running
tail -f /dev/null
