#!/bin/bash
# Entrypoint for the airflow container.
# Standalone mode auto-generates a random admin password; this wrapper
# pre-creates the admin user from AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD
# (read from .env via compose) so login credentials are deterministic and
# survive `docker compose down` + `up` as long as the postgres volume stays.
set -euo pipefail

ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
ADMIN_PASS="${AIRFLOW_ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@local}"

echo "[start.sh] Running airflow db migrate"
airflow db migrate

# Delete-then-create makes the script idempotent (password can be rotated
# just by editing .env and restarting); standalone will skip user creation
# because admin already exists after this.
echo "[start.sh] Re-creating admin user '${ADMIN_USER}' from env"
airflow users delete -u "${ADMIN_USER}" 2>/dev/null || true
airflow users create \
    --username "${ADMIN_USER}" \
    --password "${ADMIN_PASS}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email "${ADMIN_EMAIL}"

# Clean up stale PID/socket files left over from a previous container
# instance. `airflow standalone` refuses to start a webserver if any of
# these exist ("Error: Already running on PID …"), and the previous
# webserver may have crashed without removing them.
echo "[start.sh] Cleaning stale PID files"
rm -f /opt/airflow/airflow-webserver.pid \
      /opt/airflow/airflow-scheduler.pid \
      /opt/airflow/airflow-triggerer.pid \
      /opt/airflow/standalone_admin_password.txt

echo "[start.sh] Handing off to airflow standalone"
exec airflow standalone
