#!/bin/bash

if [ -z "$1" ]; then
  echo "❌ Lỗi: Thiếu tên Topic cần xem."
  echo "👉 Cách dùng: ./describe-topics.sh <tên_topic>"
  exit 1
fi

TOPIC_NAME=$1

echo "kính lúp Đang soi chi tiết topic: $TOPIC_NAME"
docker exec kafka-1 kafka-topics --describe \
  --bootstrap-server kafka-1:29092 \
  --topic "$TOPIC_NAME"