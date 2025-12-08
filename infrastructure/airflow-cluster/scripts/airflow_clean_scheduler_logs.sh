#!/bin/sh
# Giữ log hệ thống ngắn hơn (7 ngày) vì nó sinh ra liên tục
MAX_LOG_AGE_IN_DAYS=7

# Đường dẫn cụ thể vào folder con
SCHEDULER_LOG_FOLDER="./airflow/logs/scheduler"
PROCESSOR_LOG_FOLDER="./airflow/logs/dag_processor_manager"

echo "Cleaning System logs (Scheduler & Processor) older than $MAX_LOG_AGE_IN_DAYS days..."

# 1. Dọn Scheduler Logs
if [ -d "$SCHEDULER_LOG_FOLDER" ]; then
    find $SCHEDULER_LOG_FOLDER -type f -mtime +$MAX_LOG_AGE_IN_DAYS -delete
    # Xóa folder rỗng con bên trong scheduler (thường là folder ngày tháng)
    find $SCHEDULER_LOG_FOLDER -type d -empty -delete
fi

# 2. Dọn Dag Processor Logs (Nên thêm cái này)
if [ -d "$PROCESSOR_LOG_FOLDER" ]; then
    find $PROCESSOR_LOG_FOLDER -type f -mtime +$MAX_LOG_AGE_IN_DAYS -delete
    find $PROCESSOR_LOG_FOLDER -type d -empty -delete
fi

echo "Done cleaning System logs."
