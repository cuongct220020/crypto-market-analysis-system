#!/bin/sh
# Đường dẫn tính từ root project
BASE_LOG_FOLDER="./airflow/logs"
MAX_LOG_AGE_IN_DAYS=14

echo "Cleaning DAG logs (Tasks) in $BASE_LOG_FOLDER older than $MAX_LOG_AGE_IN_DAYS days..."

# 1. Xóa file log cũ, NHƯNG loại trừ thư mục scheduler và dag_processor_manager
find $BASE_LOG_FOLDER \
    -type f \
    -name '*.log' \
    -mtime +$MAX_LOG_AGE_IN_DAYS \
    -not -path "*/scheduler/*" \
    -not -path "*/dag_processor_manager/*" \
    -delete

# 2. Xóa các thư mục rỗng sau khi xóa file (để cho gọn)
find $BASE_LOG_FOLDER \
    -type d \
    -empty \
    -not -path "*/scheduler*" \
    -not -path "*/dag_processor_manager*" \
    -delete

echo "Done cleaning DAG logs."
