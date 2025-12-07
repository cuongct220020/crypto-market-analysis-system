# Pruning Logs
This folder contains 2 bash scripts for pruning old logs: 
- `airflow_clean_dag_logs.sh`: Clean logs from DAG runs that are older than 14 days
- `airflow_clean_scheduler_logs.sh`: Clean logs from schedulers that are older than 7 days

These scripts could be executed by cronjobs on every server in the cluster:
1. Make the files executable: 
```chmod +x airflow_clean_dag_logs.sh && chmod +x airflow_clean_scheduler_logs.sh```
2. Create cronjobs: 
```bash
0 0 * * * /home/infrastructure/airflow/scripts/airflow_clean_dag_logs.sh
0 10 * * * /home/infrastructure/airflow/scripts/airflow_clean_scheduler_logs.sh
```