# Dkron Prometheus Exporter

Exports Dkron json metrics in prometheus polling it thorough the dkron jobs API

## Setup

Set it up as you prefer

```shell
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configure

You could configure logging and dkron target or other bits using ENVs

```shell
export DKRON_TARGET_HOSTNAME="http://localhost:8080"
export EXPORTER_PORT="8080"
export EXPORTER_LOGLEVEL="DEBUG|debug"
```

## Run

```shell
python3 app.py
```
