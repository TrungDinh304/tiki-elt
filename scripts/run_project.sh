#!/bin/bash

# Exit on error
set -e

echo "🚀 Starting Tiki Lakehouse Project..."

# 1. Check for .env file
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Copying from .env.example..."
    cp .env.example .env
fi

# 2. Start Infrastructure
echo "🐳 Starting Docker containers..."
make up

# 3. Wait for services to be ready
echo "⏳ Waiting for services to initialize (30s)..."
# We wait a bit to ensure MinIO and Postgres are up before crawling
sleep 30

# 4. Setup environment if .venv doesn't exist
if [ ! -d .venv ]; then
    echo "📦 Setting up Python environment..."
    make setup
fi

# 5. Run Crawler
echo "🕷️  Running Tiki Crawler..."
make crawl

# 6. Run dbt transformations
echo "🔨 Running dbt transformations..."
make dbt-run

echo "✅ Project is up and running!"
echo "------------------------------------------------"
echo "🔗 MinIO Console: http://localhost:9001 (admin / minio_password)"
echo "🔗 Trino UI:      http://localhost:8080 (admin)"
echo "🔗 Airflow:       http://localhost:8081 (admin / admin)"
echo "🔗 Chatbot (RAG): http://localhost:8501"
echo "------------------------------------------------"
