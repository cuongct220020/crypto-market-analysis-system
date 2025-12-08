#!/bin/bash

echo "📋 Danh sách các Topic hiện có (đã ẩn topic hệ thống):"
echo "----------------------------------------------------"

docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092 | grep -v "^_"

echo "----------------------------------------------------"
