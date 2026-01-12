# Kafka CLI Management Tools

Tài liệu này hướng dẫn cách quản trị Kafka Topic và Data thông qua dòng lệnh (CLI), giúp tối ưu tài nguyên hệ thống khi không cần chạy Kafka UI.

## 1. Chuẩn bị (Prerequisites) 

Đảm bảo các script có quyền thực thi:

```bash
chmod +x infrastructure/kafka-cluster/scripts/*.sh
```

Tất cả các lệnh dưới đây đều thực thi thông qua container `kafka-1`. Nếu bạn muốn dùng node khác, hãy điều chỉnh tên container tương ứng.

---

## 2. Quản lý Topic (Scripts hỗ trợ)

Các script này đã được viết sẵn để đơn giản hóa các tham số dài dòng của Kafka.

### Liệt kê tất cả Topic
```bash
./infrastructure/kafka-cluster/scripts/list-topics.sh
```

### Tạo Topic mới
Mặc định: 3 Partitions, 3 Replicas.
```bash
# Cách dùng: ./create-topic.sh <tên_topic> [số_partition] [replication_factor]
./infrastructure/kafka-cluster/scripts/create-topic.sh my-new-topic 4 2
```

### Xem chi tiết Topic (Partitions, ISR, Leader)
```bash
./infrastructure/kafka-cluster/scripts/describe-topic.sh my-new-topic
```

### Xóa Topic
**Cảnh báo:** Hành động này không thể hoàn tác.
```bash
./infrastructure/kafka-cluster/scripts/delete-topic.sh my-new-topic
```

---

## 3. Thao tác với Dữ liệu (Raw CLI)

Khi cần kiểm tra dữ liệu trực tiếp bên trong topic mà không có UI.

### Console Consumer (Đọc dữ liệu)

**Đọc dữ liệu từ thời điểm hiện tại:**
```bash
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

**Đọc toàn bộ dữ liệu từ đầu (Beginning):**
```bash
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning
```

**Đọc dữ liệu Avro (Cần Schema Registry):**
Nếu topic sử dụng Avro, bạn phải dùng `kafka-avro-console-consumer` (có sẵn trong image của Confluent):
```bash
docker exec -it kafka-1 kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-avro-topic \
  --property schema.registry.url=http://schema-registry:8081 \
  --from-beginning
```

### Console Producer (Gửi dữ liệu test)
```bash
docker exec -it kafka-1 kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my-topic
# Sau đó nhập nội dung và Enter để gửi. Ctrl+C để thoát.
```

---

## 4. Quản lý Consumer Groups

Rất quan trọng để debug khi Spark Streaming hoặc Ingestion bị chậm (lag).

### Liệt kê danh sách Consumer Groups
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

### Kiểm tra tình trạng Lag (Quan trọng nhất)
Dùng để biết consumer đang xử lý đến offset nào và còn cách bao xa so với dữ liệu mới nhất.
```bash
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group <group_id>
```

### Reset Offset cho Group
Dùng khi muốn chạy lại dữ liệu từ đầu hoặc bỏ qua dữ liệu cũ. (Lưu ý: Phải stop consumer trước khi reset).
```bash
# Reset về đầu (earliest) cho một topic cụ thể
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group <group_id> \
  --topic <topic_name> \
  --reset-offsets --to-earliest --execute
```

---

## 5. Kiểm tra tình trạng Cluster

Kiểm tra ID của cluster và danh sách các broker đang online:
```bash
docker exec kafka-1 kafka-metadata-shell --snapshot /var/lib/kafka/data/meta.properties
```
Hoặc dùng lệnh đơn giản để check logs:
```bash
docker logs -f kafka-1
```
