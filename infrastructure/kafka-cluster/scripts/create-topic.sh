#!/bin/bash

# Kiểm tra xem người dùng có nhập tên topic không
if [ -z "$1" ]; then
  echo "❌ Lỗi: Thiếu tên Topic."
  echo "👉 Cách dùng: ./create-topic.sh <tên_topic> [số_partition] [replication_factor]"
  echo "   Ví dụ:     ./create-topic.sh market-data-btc"
  exit 1
fi

TOPIC_NAME=$1
PARTITIONS=${2:-3}       # Mặc định là 3 nếu không nhập
REPLICATION=${3:-3}      # Mặc định là 3 nếu không nhập

echo "⚙️  Đang tạo topic '$TOPIC_NAME' (P:$PARTITIONS, R:$REPLICATION)..."

docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic "$TOPIC_NAME" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

if [ $? -eq 0 ]; then
  echo "✅ Tạo thành công topic: $TOPIC_NAME"
else
  echo "❌ Tạo thất bại!"
fi
