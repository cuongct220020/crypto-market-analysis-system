Đây là các công thức viết theo ký hiệu toán học chuẩn cho báo cáo LaTeX:

## Công thức Trending Metrics

### 1. Price Volatility (Độ biến động giá)

$$
\text{Price Volatility} = \frac{\sigma_{\text{price}}}{\mu_{\text{price}}} \times 100
$$

Trong đó:
- $\sigma_{\text{price}} = \sqrt{\frac{1}{n-1} \sum_{i=1}^{n} (p_i - \mu_{\text{price}})^2}$ : Độ lệch chuẩn mẫu của giá
- $\mu_{\text{price}} = \frac{1}{n} \sum_{i=1}^{n} p_i$ : Giá trung bình
- $p_i$ : Giá tại thời điểm $i$ trong cửa sổ
- $n$ : Số lượng tick trong cửa sổ 5 phút

---

### 2. Window Rate of Change

$$
\text{Window ROC} = \frac{P_{\text{close}} - P_{\text{open}}}{P_{\text{open}}} \times 100
$$

Trong đó:
- $P_{\text{close}}$ : Giá đóng cửa (quan sát cuối cùng trong cửa sổ)
- $P_{\text{open}}$ : Giá mở cửa (quan sát đầu tiên trong cửa sổ)

---

### 3. Price Momentum (Động lượng giá)

$$
\text{Price Momentum} = 0.5 \times \text{Window ROC} + 0.5 \times \Delta P_{1h}
$$

Trong đó:
- $\text{Window ROC}$ : Tỷ lệ thay đổi giá trong cửa sổ 5 phút
- $\Delta P_{1h}$ : Tỷ lệ thay đổi giá trong 1 giờ (từ CoinGecko API)

---

### 4. Trending Score (Điểm xu hướng)

$$
\text{Trending Score} = 0.4 \times \text{Price Volatility} + 0.6 \times |\text{Price Momentum}|
$$

Trong đó:
- $|\text{Price Momentum}|$ : Giá trị tuyệt đối của động lượng giá
- Hệ số 0.4 và 0.6 là trọng số tương ứng

---

### 5. Các thông số cửa sổ thời gian

**Cửa sổ trượt (Sliding Window)**:
$$
W_t = [t - 5\text{ min}, t], \quad \text{sliding interval} = 1\text{ min}
$$

**Watermark** (xử lý dữ liệu đến muộn):
$$
\text{Watermark}(t) = t - 10\text{ min}
$$

---

### 6. Các chỉ số OHLC trong cửa sổ

$$
\begin{aligned}
P_{\text{open}} &= p_1 \quad \text{(giá đầu tiên)} \\
P_{\text{close}} &= p_n \quad \text{(giá cuối cùng)} \\
P_{\text{high}} &= \max_{i=1}^{n} p_i \\
P_{\text{low}} &= \min_{i=1}^{n} p_i \\
P_{\text{avg}} &= \frac{1}{n} \sum_{i=1}^{n} p_i
\end{aligned}
$$

---

### Ghi chú cho báo cáo

**Lý do sử dụng Coefficient of Variation**:
- CV chuẩn hóa độ biến động theo mức giá tuyệt đối
- Cho phép so sánh công bằng giữa các tài sản có khoảng giá khác nhau
- Ví dụ: Bitcoin (\$50,000) và Ethereum (\$3,000) có thể so sánh được

**Lý do sử dụng giá trị tuyệt đối trong Trending Score**:
- Cả xu hướng tăng mạnh ($+$) và giảm mạnh ($-$) đều được coi là "trending"
- Mục tiêu là phát hiện coin có biến động lớn, bất kể hướng