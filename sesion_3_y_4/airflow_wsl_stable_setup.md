# Running Apache Airflow Reliably in WSL

This guide explains a stable way to run **Apache Airflow inside WSL**
without frequent freezes or zombie processes.\
The goal is to ensure that:

-   Airflow processes do not get suspended by the terminal
-   The scheduler and webserver use the same configuration
-   The metadata database stays consistent
-   Development and debugging workflows are predictable

------------------------------------------------------------------------

# Core Principles

1.  Use a **fixed virtual environment**
2.  Use a **fixed AIRFLOW_HOME**
3.  Avoid suspending processes with `Ctrl+Z`
4.  Prefer **tmux** or **nohup** to keep services running
5.  Validate the metadata database before starting services

------------------------------------------------------------------------

# Fixed Paths for This Setup

    VENV=/home/arojaspa/cursobsgetl/ambientes/etl-ai-lab
    AIRFLOW_HOME=/home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow

------------------------------------------------------------------------

# Rule 1 --- Always Enter Airflow with the Same Context

Create a helper script.

``` bash
nano /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
```

Script contents:

``` bash
#!/usr/bin/env bash
source /home/arojaspa/cursobsgetl/ambientes/etl-ai-lab/bin/activate
export AIRFLOW_HOME=/home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow

echo "PYTHON      : $(which python)"
echo "AIRFLOW     : $(which airflow)"
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "DB          : $(airflow config get-value database sql_alchemy_conn)"
```

Make it executable:

``` bash
chmod +x /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
```

Before running Airflow in any terminal:

``` bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
```

This ensures CLI, scheduler, and webserver always use the same
configuration.

------------------------------------------------------------------------

# Rule 2 --- Never Suspend Airflow with Ctrl+Z

Suspending processes causes them to enter state `T` (Stopped), which
leads to zombie or frozen services.

Instead use **tmux** or **nohup**.

------------------------------------------------------------------------

# Option A --- Run Airflow with tmux (Recommended)

Install tmux:

``` bash
sudo apt update
sudo apt install -y tmux
```

Start a session:

``` bash
tmux new -s airflow
```

Window 1:

``` bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
airflow webserver --port 8080
```

Create another window:

    Ctrl+b then c

Window 2:

``` bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
airflow scheduler
```

Detach from tmux without stopping Airflow:

    Ctrl+b then d

------------------------------------------------------------------------

# Option B --- Run Airflow with nohup

``` bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh

mkdir -p $AIRFLOW_HOME/runtime

nohup airflow webserver --port 8080 > $AIRFLOW_HOME/runtime/webserver.out 2>&1 &
nohup airflow scheduler > $AIRFLOW_HOME/runtime/scheduler.out 2>&1 &
```

Check logs:

``` bash
tail -f $AIRFLOW_HOME/runtime/webserver.out
tail -f $AIRFLOW_HOME/runtime/scheduler.out
```

------------------------------------------------------------------------

# Rule 3 --- Validate the Metadata Database Before Starting

Always run:

``` bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh

airflow db check
airflow users list
```

If needed:

``` bash
airflow db migrate
```

------------------------------------------------------------------------

# Rule 4 --- Avoid SQLite for Anything Beyond Simple Testing

SQLite works for quick development but has limitations with concurrency.

For more stability, switch the Airflow metadata database to
**PostgreSQL**.

------------------------------------------------------------------------

# Installing PostgreSQL in WSL

``` bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib
```

Create database and user:

``` bash
sudo -u postgres psql
```

``` sql
CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'ClaveSegura2026!';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
\q
```

Install driver:

``` bash
source /home/arojaspa/cursobsgetl/ambientes/etl-ai-lab/bin/activate
pip install psycopg2-binary
```

Configure Airflow connection:

``` bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://airflow_user:ClaveSegura2026!@localhost/airflow_db'
```

Initialize database:

``` bash
airflow db migrate
```

------------------------------------------------------------------------

# Rule 5 --- Use Startup Scripts

Create scripts to start services safely.

## start_webserver.sh

``` bash
#!/usr/bin/env bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
mkdir -p $AIRFLOW_HOME/runtime
airflow db check || exit 1
exec airflow webserver --port 8080
```

## start_scheduler.sh

``` bash
#!/usr/bin/env bash
source /home/arojaspa/cursobsgetl/codigo/etl-ai-lab/airflow_env.sh
mkdir -p $AIRFLOW_HOME/runtime
airflow db check || exit 1
exec airflow scheduler
```

Make executable:

``` bash
chmod +x start_webserver.sh
chmod +x start_scheduler.sh
```

------------------------------------------------------------------------

# Rule 6 --- Debug DAGs Without Running the Full Stack

For quick testing you can use:

``` python
dag.test()
```

This runs a DAG locally without starting scheduler or webserver.

------------------------------------------------------------------------

# Rule 7 --- Clean Shutdown Procedure

To stop Airflow safely:

``` bash
pkill -9 -f "airflow scheduler" || true
pkill -9 -f "airflow webserver" || true
pkill -9 -f "airflow" || true
```

If WSL becomes unstable:

``` powershell
wsl --shutdown
```

------------------------------------------------------------------------

# Recommended Workflow

    source airflow_env.sh
    airflow db check
    tmux new -s airflow

Window 1:

    ./start_webserver.sh

Window 2:

    ./start_scheduler.sh

------------------------------------------------------------------------

# Final Advice

Stable Airflow in WSL requires:

-   one virtual environment
-   one AIRFLOW_HOME
-   a reliable metadata database
-   process management via tmux or nohup

For serious workloads:

-   PostgreSQL metastore
-   LocalExecutor
-   reproducible startup scripts
