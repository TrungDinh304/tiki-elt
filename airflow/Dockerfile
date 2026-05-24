ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
# Install OS deps + uv resolver, then create a SEPARATE venv for crawler/dbt
# so dbt-core's pins don't fight Airflow's baked-in constraints. Done as root
# so /opt/ is writable; binaries inside the venv stay world-executable so the
# airflow user can run them at task-time.
ENV PROJECT_VENV=/opt/project-venv
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && /usr/local/bin/uv venv ${PROJECT_VENV} \
    && /usr/local/bin/uv pip install --python ${PROJECT_VENV}/bin/python --no-cache \
        "requests>=2.32.3" \
        "chardet>=5.2.0,<6" \
        "pandas==2.2.1" \
        "boto3==1.34.69" \
        "python-dotenv==1.0.1" \
        "pyarrow==15.0.2" \
        "duckdb" \
        "dbt-duckdb" \
        "matplotlib" \
        "seaborn" \
    # RAG indexer deps. Install CPU-only torch from the PyTorch CPU index first
    # so sentence-transformers doesn't pull the ~2GB CUDA wheel from PyPI.
    && /usr/local/bin/uv pip install --python ${PROJECT_VENV}/bin/python --no-cache \
        --index-url https://download.pytorch.org/whl/cpu \
        "torch>=2.2" \
    && /usr/local/bin/uv pip install --python ${PROJECT_VENV}/bin/python --no-cache \
        "psycopg2-binary>=2.9" \
        "pgvector>=0.2.5" \
        "sentence-transformers>=2.7" \
    && chmod -R a+rx ${PROJECT_VENV}

# Wrapper that pre-creates the admin user from env before handing off to
# `airflow standalone` (standalone otherwise generates a random password).
COPY --chmod=0755 airflow/start.sh /opt/airflow/start.sh

USER airflow
WORKDIR /opt/project
