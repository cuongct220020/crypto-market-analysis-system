# Thiết kế Serving Layer (Elasticsearch & Kibana)

## 1. Elasticsearch & Kibana (Serving Layer)

Trong kiến trúc Lambda, **Serving Layer** đóng vai trò cung cấp dữ liệu cho các truy vấn phân tích và hiển thị dashboard với độ trễ thấp. Serving Layer phải đáp ứng các yêu cầu sau:

*   Truy vấn nhanh trên tập dữ liệu lớn bao gồm **các giao dịch cá voi (whale transactions)**, chỉ số thị trường và thông tin xu hướng blockchain.
*   Hỗ trợ tìm kiếm đa chiều, đặc biệt theo địa chỉ ví **cá mập/cá voi**, mã băm giao dịch **giá trị lớn** và hợp đồng thông minh.
*   Cung cấp các phép phân tích thống kê như tổng số giao dịch, khối lượng token và xu hướng theo thời gian.
*   Cập nhật dữ liệu gần thời gian thực để bảng điều khiển phản ánh chính xác trạng thái mới nhất của xu hướng thị trường.
*   Tích hợp dễ dàng với các hệ thống trực quan hóa dữ liệu.

Dữ liệu trong hệ thống có nguồn gốc từ cả **Batch Layer** (ClickHouse - lưu trữ toàn vẹn lịch sử) và **Speed Layer** (dữ liệu streaming - lưu trữ các tín hiệu thị trường). Serving Layer (Elasticsearch) tập trung vào việc lưu trữ dữ liệu đã qua xử lý (aggregated metrics) và dữ liệu dị thường (anomalies/alerts) để phục vụ tra cứu nhanh.

## 2. Elasticsearch (Search Engine)

**Elasticsearch** là một cơ sở dữ liệu NoSQL hướng tài liệu (document-oriented) được xây dựng trên thư viện tìm kiếm full-text Apache Lucene, trong đó mỗi bản ghi được lưu trữ dưới dạng tài liệu JSON. Mô hình dữ liệu này phù hợp với đặc điểm của dữ liệu metric và logs sự kiện.

Elasticsearch hỗ trợ cơ chế gần thời gian thực (**Near Real-time**), cho phép dữ liệu mới từ Spark Streaming được đưa vào chỉ mục và có thể truy vấn chỉ sau khoảng thời gian ngắn (khoảng 1 giây). Điều này phù hợp với đặc điểm của blockchain, nơi các tín hiệu thị trường cần được phát hiện ngay lập tức.

Với khối lượng dữ liệu lớn, nhu cầu phân tích thống kê và cập nhật gần thời gian thực, Elasticsearch là lựa chọn phù hợp cho Serving Layer, tập trung vào việc lưu trữ và phục vụ **các chỉ số phân tích thị trường (market metrics)** và **dữ liệu giao dịch trọng yếu (whale alerts)** được chắt lọc từ toàn bộ mạng lưới.

## 3. Kibana (Analytic Dashboard)

**Kibana** là công cụ trực quan hóa dữ liệu được phát triển chuyên biệt cho Elasticsearch, cung cấp giao diện đồ họa để xây dựng dashboard, biểu đồ và bảng phân tích. Trong hệ thống phân tích Ethereum, Kibana đóng vai trò là tầng giao diện (presentation layer).

Trong kiến trúc Lambda của hệ thống, Kibana đóng vai trò là tầng trực quan hóa (visualization layer) của Serving Layer với các chức năng chính:

*   Kết nối trực tiếp với Elasticsearch để truy xuất dữ liệu đã được xử lý từ Batch Layer (trend dài hạn) hoặc dữ liệu luồng từ Speed Layer (trend realtime).
*   Hiển thị dữ liệu gần thời gian thực cho bảng điều khiển theo các chỉ số quan trọng: biến động giá, khối lượng giao dịch cá voi, và điểm số xu hướng (trending score).
*   Cung cấp giao diện trực quan cho phép người dùng lọc, tìm kiếm và phân tích các tín hiệu thị trường.

## 4. Các loại biểu đồ và chức năng trực quan hóa

### 4.1. Biểu đồ xu hướng theo thời gian

Biểu đồ đường (line chart) và biểu đồ diện tích (area chart) được sử dụng để hiển thị xu hướng giá, biến động (volatility) hoặc khối lượng token theo thời gian. Khi người dùng tạo biểu đồ dạng chuỗi thời gian (time-series), Kibana gửi các truy vấn tổng hợp dữ liệu theo thời gian (date histogram aggregation) tới Elasticsearch.

Nhờ cơ chế Near Real-time, biểu đồ được cập nhật liên tục khi dữ liệu mới từ Speed Layer xuất hiện, cho phép người dùng theo dõi nhịp đập thị trường (market pulse) gần như tức thời.

### 4.2. Biểu đồ phân phối và thống kê

Biểu đồ cột (bar chart) và biểu đồ tần suất (histogram) được sử dụng để thống kê phân bổ dòng tiền theo địa chỉ ví hoặc phân loại token.

Quá trình tạo biểu đồ cột thực hiện các bước sau:
1.  Elasticsearch sử dụng `terms aggregation` để nhóm các tài liệu theo giá trị của trường được chỉ định (ví dụ: Token Symbol).
2.  Tính toán số lượng hoặc tổng giá trị trong mỗi nhóm.
3.  Hợp nhất kết quả và sắp xếp theo tiêu chí được chỉ định (ví dụ: Top các đồng coin có biến động mạnh nhất).

### 4.3. Bảng dữ liệu chi tiết (Whale Alerts)

Bảng dữ liệu (data table) liệt kê chi tiết **các giao dịch cá voi và sự kiện thị trường**, hỗ trợ truy vết nhanh dòng tiền theo địa chỉ ví hoặc mã băm giao dịch. Kibana sử dụng truy vấn cấp tài liệu (document-level search) dựa trên chỉ mục đảo ngược của Elasticsearch, cho phép tìm kiếm các giao dịch **có giá trị lớn** cụ thể trong tập dữ liệu với thời gian phản hồi ở mức mili giây.

Đặc điểm của bảng dữ liệu:
*   Hỗ trợ tìm kiếm full-text trên các trường như địa chỉ ví **của các nhà đầu tư lớn**, mã băm giao dịch **đáng ngờ**.
*   Cập nhật tự động sau khoảng 1 giây khi Elasticsearch thực hiện thao tác làm mới (refresh).
*   Cho phép sắp xếp, lọc và phân trang dữ liệu **để phát hiện xu hướng gom/xả hàng (accumulation/dumping)** một cách linh hoạt.

### 4.4. Bản đồ nhiệt và chỉ số tổng hợp

Bản đồ nhiệt (heatmap) và các chỉ số tổng hợp (metric visualization) được sử dụng để phát hiện các hoạt động bất thường.

*   **Bản đồ nhiệt:** Kết hợp aggregation theo hai chiều (ví dụ: Thời gian và Mức độ biến động) để hiển thị các vùng "nóng" của thị trường.
*   **Chỉ số tổng hợp:** Hiển thị các con số quan trọng (KPIs) như: Giá trị giao dịch cá voi 24h, Số lượng tín hiệu cảnh báo, Trending Score hiện tại.
