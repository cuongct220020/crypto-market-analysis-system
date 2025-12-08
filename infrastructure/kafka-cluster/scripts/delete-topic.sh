#!/bin/bash

# Kiểm tra tham số đầu vào
if [ -z "$1" ]; then
  echo "❌ Lỗi: Thiếu tên Topic cần xoá."
  echo "👉 Cách dùng: ./delete-topic.sh <tên_topic>"
  exit 1
fi

TOPIC_NAME=$1

# Cảnh báo an toàn (Safety Check)
echo "⚠️  CẢNH BÁO: Hành động này sẽ xoá vĩnh viễn topic '$TOPIC_NAME' và toàn bộ dữ liệu bên trong!"
read -p "❓ Bạn có chắc chắn muốn tiếp tục không? (y/N): " -n 1 -r
echo "" # Xuống dòng cho đẹp

# Kiểm tra câu trả lời (chấp nhận y hoặc Y)
if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo "🗑️  Đang gửi lệnh xoá topic '$TOPIC_NAME'..."

  docker exec kafka-1 kafka-topics --delete \
    --bootstrap-server kafka-1:29092 \
    --topic "$TOPIC_NAME"

  if [ $? -eq 0 ]; then
    echo "✅ Đã gửi lệnh xoá thành công."
    echo "ℹ️  Lưu ý: Kafka xoá topic theo cơ chế bất đồng bộ (async). Topic có thể vẫn hiện trong list một vài giây trước khi biến mất hẳn."
  else
    echo "❌ Lỗi: Không thể xoá topic (có thể topic không tồn tại)."
  fi
else
  echo "🚫 Đã huỷ thao tác xoá."
fi
