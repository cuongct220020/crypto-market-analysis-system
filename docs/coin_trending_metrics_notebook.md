## 1. Phân tích biến động giá (Price Volarity)
### 1.1. Bản chất kinh tế học và định nghĩa

Trong lý thuyết tài chính hiện đại, biến động giá (Price Volatility) được định nghĩa là thước đo thống kê sự phân tán của lợi nhuận đối với một chứng khoán hoặc chỉ số thị trường nhất định. 
Trong không gian tiền mã hoá, **chỉ số này phản ánh mức độ và tốc độ thay đổi giá của một tài sản trong khung thời gian 1 giờ**. Cần lưu ý rằng biến động là một vector vô hướng trong ngữ cảnh rủi ro;
nó không chỉ ra hướng di chuyển của giá (tăng hay giảm) mà chỉ đo lường cường độ của sự di chuyển đó. 

Một tài sản có `Price Volatility` thấp thường biểu thị trạng thái cân bằng tạm thời giữa cung và cầu, hoặc sự thiếu quan tâm tới thị trường (thanh khoản thấp). 
Ngược lại `Price Volatility` cao biểu thị sự bất định lớn, nơi tâm lý thị trường đang dao động mạnh giữa sợ hãi (fear) và tham lam (greed). Mặc dù các nhà đầu tư truyền 
thống thường coi biến động là rủi ro cần phòng ngừa, nhưng trong thị trường crypto, biến động chính là nguồn gốc của lợi nhuận phi thường (alpha) và cũng là nguyên nhân của khoản lỗ lớn. 

**Tham khảo:** 
* https://finst.com/en/learn/articles/what-is-volatility

### 1.2. Phương pháp tính toán và mô hình toán học
Để định lượng Price Volatility, các hệ thống thường sử dụng hai phương pháp chính: **Độ lệch chuẩn (Standard Deviation)** và **phạm vi thực trung bình (Average True Range - ATR)**.  

#### 1.2.1. Độ lệch chuẩn (Standard Deviation)

Đây là phương pháp phổ biến nhất, đo lường mức độ phân tán của giá so với giá trị trung bình trong một cửa sổ thời gian trượt (rolling window). 

Công thức tính toán cho độ biến động dựa trên lợi suất logarit ($R_t$):

$$R_t = \ln\left(\frac{P_t}{P_{t-1}}\right)$$

Độ biến động ($\sigma$) trong khung giờ được tính như sau:

$$\sigma = \sqrt{\frac{1}{N-1} \sum_{i=1}^{N} (R_i - \bar{R})^2}$$

Trong đó $N$ là số lượng mẫu quan sát (ví dụ: 60 phút trong 1 giờ) và $\bar{R}$ là lợi suất trung bình. Phương pháp này giả định phân phối chuẩn của lợi suất, mặc dù thực tế dữ liệu crypto thường có phân phối đuôi béo (fat-tailed distribution).

#### 1.2.2. Average True Range (ATR)

ATR là một chỉ số hiệu quả hơn trong việc đo lường biến động của thị trường crypto vì nó tính đến cả các khoảng trống giá (gaps) – hiện tượng thường xuyên xảy ra trong các thị trường biến động mạnh.

$$TR_t = \max(High_t - Low_t, |High_t - Close_{t-1}|, |Low_t - Close_{t-1}|)$$

$$ATR_t = \frac{ATR_{t-1} \times (n-1) + TR_t}{n}$$

Chỉ số `price_volatility` thường là giá trị chuẩn hóa của ATR hoặc độ lệch chuẩn, cho phép so sánh độ biến động giữa các tài sản có mệnh giá khác nhau.


## 2. Phân tích động lượng giá (Price Momentum)

### 2.1. Cơ học của chuyển động giá

Động lượng (Price Momentum) đo lường tốc độ gia tốc của giá trị tài sản. Nếu ví thị trường như một chiếc xe đang chạy, thì price là vị trí, volatility là tốc độ, và momentum là gia tốc. 
Chỉ số này trả lời câu hỏi cốt lõi: **"Xu hướng hiện tại mạnh đến mức nào và liệu nó có khả năng tiếp diễn hay đang suy yếu?"**. Trong thị trường crypto, momentum thường được thúc đẩy bởi tâm lý đám đông (FOMO - Fear Of Missing Out) và các dòng vốn đầu cơ ngắn hạn. 

### 2.2 Chỉ Báo Động Lượng (Momentum Indicators) Trong Phân Tích Crypto

### 2.2.1. RSI (Relative Strength Index) - Chỉ Số Sức Mạnh Tương Đối

#### Công thức tính toán
$$RSI = 100 - \frac{100}{1 + RS}$$

Trong đó: $RS = \frac{\text{Trung bình tăng giá n phiên}}{\text{Trung bình giảm giá n phiên}}$

Thông thường n = 14 phiên (có thể điều chỉnh thành 7 cho khung ngắn hạn hoặc 21 cho dài hạn).

#### Cơ chế hoạt động
RSI đo lường **tốc độ và độ lớn** của các biến động giá gần đây để xác định điều kiện quá mua hoặc quá bán. Khác với các chỉ báo theo xu hướng, RSI là oscillator dao động trong biên độ cố định (0-100), giúp nhận diện các điểm đảo chiều tiềm năng.

#### Ứng dụng chuyên sâu trong Crypto

**A. Xác định vùng quá mua/quá bán**
- **RSI > 70**: Vùng quá mua - Thị trường có thể sắp điều chỉnh giảm
- **RSI < 30**: Vùng quá bán - Cơ hội mua vào tiềm năng
- **Lưu ý crypto**: Do tính biến động cao, nhiều trader crypto sử dụng ngưỡng **RSI > 80** và **RSI < 20** để lọc nhiễu tốt hơn

**B. Phân kỳ (Divergence) - Tín hiệu mạnh nhất**
- **Phân kỳ âm (Bearish)**: Giá tạo đỉnh cao hơn nhưng RSI tạo đỉnh thấp hơn → Xu hướng tăng đang suy yếu
- **Phân kỳ dương (Bullish)**: Giá tạo đáy thấp hơn nhưng RSI tạo đáy cao hơn → Xu hướng giảm đang mất lực

**C. Chiến lược giao dịch thực tế**
```
Entry Long:
- RSI < 30 trên khung 1H
- Kết hợp với hỗ trợ kỹ thuật hoặc Fibonacci retracement 0.618
- Đợi RSI vượt ngược lên trên 30 (xác nhận đảo chiều)

Exit/Stop Loss:
- RSI đạt > 70 (chốt lời từng phần)
- Hoặc phá vỡ đường hỗ trợ chính
```

**D. Hạn chế cần lưu ý**
- Trong xu hướng mạnh (bull run hoặc bear market sâu), RSI có thể nằm ở vùng cực trị trong thời gian dài
- RSI không cho biết **khi nào** sẽ đảo chiều, chỉ cho biết **điều kiện** đảo chiều
- Cần kết hợp với volume và price action để xác nhận

---

### 2.2.2 MACD (Moving Average Convergence Divergence)

#### Công thức tính toán
$$MACD = EMA_{12} - EMA_{26}$$
$$Signal\ Line = EMA_9(MACD)$$
$$Histogram = MACD - Signal\ Line$$

#### Cơ chế hoạt động
MACD theo dõi mối quan hệ giữa hai đường trung bình động hàm mũ (EMA) để phát hiện sự thay đổi trong động lượng giá. Khi đường MACD cắt qua Signal Line, nó báo hiệu sự dịch chuyển trong cán cân cung-cầu.

#### Ứng dụng chuyên sâu trong Crypto

**A. Tín hiệu giao cắt (Crossover)**
- **Bullish Crossover**: MACD cắt lên trên Signal Line → Tín hiệu mua
- **Bearish Crossover**: MACD cắt xuống dưới Signal Line → Tín hiệu bán
- **Zero Line Cross**: 
  - MACD cắt lên trên 0: Xác nhận xu hướng tăng mạnh mẽ
  - MACD cắt xuống dưới 0: Xác nhận xu hướng giảm

**B. Histogram - Đo lường sức mạnh xu hướng**
- Histogram dương và **mở rộng**: Động lượng tăng đang gia tốc
- Histogram dương nhưng **thu hẹp**: Động lượng tăng đang yếu đi (cảnh báo sớm)
- Histogram âm và **mở rộng**: Áp lực bán đang tăng
- Histogram âm nhưng **thu hẹp**: Áp lực bán đang giảm (có thể đảo chiều)

**C. Phân kỳ MACD - Tín hiệu đảo chiều mạnh**
```
Phân kỳ âm (Bearish Divergence):
- Giá: Đỉnh 1 → Đỉnh 2 (cao hơn)
- MACD: Đỉnh 1 → Đỉnh 2 (thấp hơn)
→ Momentum đang suy yếu dù giá vẫn tăng → Chuẩn bị cho đợt giảm

Phân kỳ dương (Bullish Divergence):
- Giá: Đáy 1 → Đáy 2 (thấp hơn)
- MACD: Đáy 1 → Đáy 2 (cao hơn)
→ Áp lực bán đang giảm → Chuẩn bị cho đợt tăng
```

**D. Chiến lược giao dịch MACD cho Crypto**
```
Setup mua (Long):
1. Xác định xu hướng tăng tổng thể (MACD > 0)
2. Đợi pullback (MACD histogram thu hẹp)
3. Entry khi MACD cắt lên Signal Line
4. Stop loss dưới đáy gần nhất
5. Target: Khi histogram bắt đầu thu hẹp lại

Setup bán (Short):
1. Xác định xu hướng giảm (MACD < 0)
2. Đợi rebound (MACD histogram âm thu hẹp)
3. Entry khi MACD cắt xuống Signal Line
4. Stop loss trên đỉnh gần nhất
```

**E. Tối ưu hóa cho thị trường Crypto**
- **Timeframe ngắn (1H-4H)**: MACD(8,17,9) - Phản ứng nhanh hơn với biến động
- **Timeframe trung bình (4H-1D)**: MACD(12,26,9) - Chuẩn
- **Timeframe dài (1D-1W)**: MACD(19,39,9) - Lọc nhiễu tốt hơn

**F. Hạn chế**
- Là chỉ báo trễ (lagging indicator) - Thường xác nhận xu hướng khi đã diễn ra
- Tạo nhiều tín hiệu sai trong thị trường sideway
- Cần kết hợp với chỉ báo xu hướng (MA, Bollinger Bands)

---

### 2.2.3. ROC (Rate of Change) - Tốc Độ Thay Đổi

#### Công thức tính toán
$$ROC = \frac{P_t - P_{t-n}}{P_{t-n}} \times 100$$

Trong đó:
- $P_t$: Giá hiện tại
- $P_{t-n}$: Giá n phiên trước đó
- n thường = 12 hoặc 14 phiên

#### Cơ chế hoạt động
ROC đo lường **phần trăm thay đổi thuần túy** của giá trong một khoảng thời gian cụ thể. Khác với RSI hay MACD có tính làm mượt (smoothing), ROC phản ánh momentum **thô** và **trực tiếp** hơn.

#### Ứng dụng chuyên sâu trong Crypto

**A. Xác định momentum tăng tốc**
- **ROC > 0**: Giá hiện tại cao hơn n phiên trước → Xu hướng tăng
- **ROC < 0**: Giá hiện tại thấp hơn n phiên trước → Xu hướng giảm
- **ROC tăng dần** (ví dụ: +5% → +8% → +12%): Momentum đang gia tốc mạnh
- **ROC dương nhưng giảm dần** (+15% → +10% → +5%): Momentum đang chậm lại

**A. Phát hiện sớm điểm đảo chiều**
```
Tín hiệu Bullish:
- ROC cắt lên trên 0 từ vùng âm
- Đặc biệt mạnh khi ROC < -10% sau đó bật lên

Tín hiệu Bearish:
- ROC cắt xuống dưới 0 từ vùng dương
- Cảnh báo mạnh khi ROC > +15% sau đó quay đầu
```

**C. So sánh với ngưỡng lịch sử**
Trong crypto, bạn có thể thiết lập ngưỡng động:
- **ROC > +20%**: Vùng mua quá nóng (overbought) → Cân nhắc chốt lời
- **ROC < -20%**: Vùng bán quá sâu (oversold) → Tìm cơ hội mua
- Các ngưỡng này cần điều chỉnh theo từng coin (BTC thường ít biến động hơn altcoin)

**D. Chiến lược giao dịch ROC**
```
Chiến lược Momentum Breakout:
1. Quan sát ROC dao động quanh 0 (consolidation)
2. Entry khi ROC vượt +5% với volume tăng đột biến
3. Hold khi ROC tiếp tục tăng và > 0
4. Exit khi ROC đạt đỉnh và bắt đầu quay đầu giảm
5. Stop loss khi ROC cắt xuống dưới 0

Chiến lược Mean Reversion:
1. Khi ROC < -15% (oversold cực đoan)
2. Đợi ROC bật lên và cắt qua MA(5) của chính nó
3. Entry long với kỳ vọng giá hồi về trung bình
4. Target: ROC về 0 hoặc vùng dương
```

**E. Kết hợp ROC với các chỉ báo khác**
```
ROC + Volume:
- ROC tăng + Volume tăng = Xu hướng mạnh và bền vững
- ROC tăng + Volume giảm = Cảnh báo momentum yếu

ROC + RSI:
- ROC > +10% và RSI > 70 = Quá nóng, cân nhắc chốt
- ROC < -10% và RSI < 30 = Oversold sâu, tìm điểm vào

ROC + MACD:
- ROC dương + MACD bullish crossover = Xác nhận mạnh mẽ
- ROC âm + MACD bearish crossover = Tránh entry
```

**F. Ưu điểm của ROC trong Crypto**
- **Phản ứng nhanh**: Không có độ trễ như MACD
- **Đơn giản**: Dễ tính toán và diễn giải
- **Phát hiện pump & dump**: ROC tăng đột biến (+50% trong 1H) thường là dấu hiệu pump
- **Đo lường FOMO**: ROC cực dương (> +30%) thường đi kèm tâm lý FOMO

**G. Hạn chế**
- **Nhiễu cao**: Vì không có smoothing, ROC rất nhạy với biến động giá ngắn hạn
- **Whipsaw**: Dễ cho tín hiệu sai trong thị trường sideway
- **Không cho biết mức giá tuyệt đối**: Chỉ cho biết % thay đổi, không phản ánh mức hỗ trợ/kháng cự


## Lưu Ý Quan Trọng Cho Trader Crypto

1. **Không có chỉ báo nào hoàn hảo 100%**: Luôn kết hợp ít nhất 2-3 chỉ báo và xác nhận bằng price action
2. **Điều chỉnh tham số**: Thị trường crypto biến động cao hơn cổ phiếu, cần tối ưu lại các tham số mặc định
3. **Backtesting**: Test chiến lược trên dữ liệu lịch sử trước khi áp dụng thực tế
4. **Risk Management**: Kỹ thuật chỉ chiếm 30%, quản trị rủi ro và tâm lý chiếm 70%
5. **Timeframe correlation**: Luôn check tín hiệu trên timeframe cao hơn (1D) trước khi trade trên timeframe thấp (1H)

#### 2.3. Hiện tượng phân kỳ (Divergence) và tín hiệu cảnh báo
Một trong những khía cạnh quan trọng nhất của việc phân tích `price_momentum` là hiện tượng phân kỳ. 
Khi giá của một tài sản tiếp tục tạo đỉnh mới (Higher High) nhưng chỉ số động lượng (như RSI hoặc MACD) lại tạo đỉnh thấp hơn (Lower High),
đây gọi là phân kỳ âm (Bearish Divergence). Điều này chỉ ra rằng mặc dù giá đang tăng, nhưng lực mua (buying pressure) đang suy yếu dần và một sự đảo chiều sụt giảm là rất có khả năng xảy ra.

Ngược lại, Phân kỳ dương (Bullish Divergence) xảy ra khi giá tạo đáy mới thấp hơn nhưng động lượng tạo đáy cao hơn, báo hiệu lực bán đã cạn kiệt và phe mua đang bắt đầu tham gia thị trường. 
Trong các thuật toán giao dịch tự động, việc phát hiện phân kỳ momentum thường là điều kiện tiên quyết để kích hoạt các lệnh đảo chiều (mean reversion strategies). 

#### 2.4. Momentum Crash và rủi ro đuôi

Nghiên cứu học thuật chỉ ra rằng các chiến lược giao dịch theo động lượng (Momentum Strategies) trong crypto mang lại lợi nhuận cao nhưng đi kèm với rủi ro "Momentum Crash" - khi xu hướng đảo chiều đột ngột và dữ dội.
Điều này thường xảy ra khi `price_momentum` đạt đến mức cực đoan (ví dụ RSI > 90 trên khung 1H), kích hoạt các lệnh chốt lời tự động của cá voi, dẫn đến hiệu ứng domino giảm giá. 

## 3. Phân tích Volume Metrics

Khối lượng giao dịch "xương sống" của mọi phân tích kĩ thuật. Giá có thể bị thao túng dễ dàng trong ngắn hạn, nhưng khối lượng đòi hỏi nguồn vốn thực tế để thực hiện, do đó khó làm giả hơn (trừ trường hợp Wash Trading). 

### 3.1. Khối lượng trung bình (Volume Avg)

`volume_avg` đóng vai trò là đường cơ sở (baseline) để chuẩn hoá dữ liệu. Do tính chất chu kỳ của thị trường (ví dụ: khối lượng thường thấp vào cuối tuần hoặc giờ nghỉ trưa của các thị trường tài chính lớn), 
việc so sánh khối lượng hiện tại với một con số tuyệt đối là vô nghĩa. Thay vào đó, volume_avg thường được tính bằng Đường trung bình động đơn giản (SMA) của khối lượng trong 20 hoặc 24 giờ gần nhất ($SMA_{20}$).

$$Volume\_Avg = \frac{1}{n} \sum_{i=1}^{n} Volume_i$$

### 3.2. Tỷ lệ đột biến khối lượng (Volume Spike Ratio)

Đây là chỉ số quan trọng nhất phát hiện các cú sốc thong tin hoặc sự tham gia của dòng tiền lớn. 

$$Volume\_Spike\_Ratio = \frac{Volume_{current}}{Volume\_Avg}$$

#### 3.2.1. Phân loại ngưỡng và ý nghĩa

* **Ratio xấp xỉ 1.0:** Thị trường ổn định, dòng tiền bán lẻ chiếm ưu thế. 
* **Ratio > 2.5:** Đột biến đáng chú ý (Significant Spike). Thường xuất hiện tại các điểm hỗ trợ/kháng cự quan trọng. Đây là dấu hiệu của việc các lệnh chờ (limit orders) lớn đang được khớp. 
* **Ratio > 5.0 - 10.0:** Đột biến cực đoan (Extreme Spike). Dấu hiệu của sự kiện tin tức lớn (như niêm yết sàn mới, hack, hoặc thay đổi kế hoạch) hoặc hành động "pump" của cá voi. 

#### 3.2.2. Phân tích kết hợp giá và khối lượng (Volume-Price Analysis - VPA)

Việc giải thích `volume_spike_ratio` phải luôn đi kèm với hành động giá (price action):

1. **Giá tăng + Volume Spike:** Xác nhận xu hướng tăng bền vững. Phe mua chấp nhận trả giá cao hơn với khối lượng lớn. 
2. **Giá tăng + Volume Thấp (Ratio < 1):** Tăng giá ảo (Fakeout). Không có sự tham gia của dòng tiền lơns, giá dễ dàng đảo chiều khi gặp kháng cự. 
3. **Giá đi ngang + Volume Spike:** Dấu hiệu của sự chuyển giao hàng hoá (Hand-over). Cá voi có thể đang âm thầm xả hàng (Distribution) cho nhỏ lể ở mức giá cao, hoặc gom hàng (Accumulation) ở mức giá thấp mà không đẩy giá chạy. 

### 3.3. Phát hiện giao dịch rửa (Wash Trading Detection)

Một trong những thách thức lớn nhất của dữ liệu khối lượng trong crypto là nạn Wash Trading - hành vi tự mua tự bán để tạo thanh khoản ảo.
Các nghiên cứu từ Chainanalysis và Solidus Labs đã chỉ ra các phương pháp heuristics để lọc khối lượng ảo này, vốn cần được áp dụng khi tính toán `volume_avg` và `volume_spike_ratio` thực tế:
* **Mô hình A-A:** Một địa chỉ ví thực hiện cả lệnh mua và bán đối ứng trong một khoảng thời gian ngắn. 
* **Mô hình Đa bên (Multi-party):** Một nhóm ví (sybil wallets) giao dịch qua lại với nhau.
* **Ngưỡng phát hiện:** Các giao dịch có chênh lệch lợi nhuận < 1% thực hiện trong vòng 25 blocks (khoảng 5 phút) giữa cùng một cặp ví thường bị nghi ngờ là wash trading. 
Nếu một đồng coin có `volume_spike_ratio` cao nhưng số lượng địa chỉ ví hoạt động (active addresses) không tăng tương ứng, khả năng cao đó là wash trading.

Tham khảo:
* https://arxiv.org/html/2311.18717v3
* https://www.chainalysis.com/blog/crypto-market-manipulation-wash-trading-pump-and-dump-2025/
* https://www.soliduslabs.com/reports/crypto-wash-trading

## 4. Phân tích hành vi Cá Voi (Whale Metrics)

Trong hệ sinh thái crypto, "Cá voi" (Whales) là các thực thể nắm giữ lượng tài sản đủ lớn để tác động lên giá thị trường. Theo dõi hành vi của họ là chìa khóa để dự đoán xu hướng, vì dòng tiền thông minh (Smart Money) thường đi trước dòng tiền bán lẻ.

### 4.1. Định nghĩa ngưỡng cá voi
Không có mọt con số cố định cho mọi đồng coin. Theo Santiment, ngưỡng cá voi nên được xác định dựa trên thang đo logarit của vốn hoá thị trường:

* Vốn hoá > 100 tỷ USD (như BTC): Cá voi là ví giữ > $16.6 triệu. 
* Vốn hóa > 10 tỷ USD: Cá voi là ví giữ > $2 triệu.
* Vốn hóa > 100 triệu USD: Cá voi là ví giữ > $200k.

### 4.2. Phân biệt Whale Tx Count và Whale Volume


#### 4.2.1. Số lượng giao dịch cá voi (Whale Tx count)

Chỉ số này đếm số lần xuất hiện các giao dịch vượt ngưỡng quy định trong một giờ.

* **Ý nghĩa:** Đo lường tần suất hoạt động. Một sự gia tăng đột ngột trong `whale_tx_count` (ví dụ: từ 5 tx/giờ lên 50 tx/giờ) là tín hiệu cảnh báo sớm rằng "những gã khổng lồ" đang thức giấc.
* **Hành vi chia nhỏ lệnh (Iceberg Orders):** Cá voi hiếm khi đặt một lệnh khổng lồ duy nhất vì sẽ gây trượt giá. Họ thường dùng bot để chia nhỏ lệnh mua \$10 triệu thành 100 lệnh $100k. Do đó, whale_tx_count cao thường phản ánh chính xác hoạt động chia nhỏ này hơn là whale_volume đơn lẻ

#### 4.2.2. Khối lượng giao dịch cá voi (Whale Volume)
Chỉ số này tính tổng giá trị USD của giao dịch cá voi. **Ý nghĩa:** Đo lường cường độ kinh tế. Nó cho biết bao nhiêu áp lực mua hoặc bán thực tế đang được đưa vào thị trường.

Tham khảo:
* https://www.ledger.com/academy/topics/crypto/how-to-track-crypto-whale-movements
* https://www.binance.com/en/square/post/34721315252857
* https://www.nansen.ai/post/how-to-analyze-token-whale-holder-patterns-using-crypto-analysis-tools-for-smarter-trading

### 4.3. Phân tích dòng chảy (Flow Analysis): Inflow vs Outflow

Để hiểu tác động của `whale_volume`, cần phân loại hướng di chuyển của dòng tiền:

| Loại Giao dịch | Hành vi quan sát được | Tác động dự kiến lên giá |
|---|---|---|
| **Exchange Inflow** (Ví -> Sàn) | Cá voi chuyển tiền từ ví lạnh lên sàn giao dịch. | **Tiêu cực (Bearish)**: Tăng áp lực bán tiềm năng. Chuẩn bị chốt lời hoặc mở vị thế Short. |
| **Exchange Outflow** (Sàn -> Ví) | Cá voi rút tiền từ sàn về ví cá nhân/ví lạnh. | **Tích cực (Bullish)**: Giảm nguồn cung lưu thông. Tín hiệu nắm giữ dài hạn (HODL) hoặc tích lũy. |
| **Inter-wallet Transfer** | Chuyển tiền giữa các ví cá nhân hoặc OTC. | **Trung lập**: Có thể là tái cơ cấu danh mục hoặc giao dịch thỏa thuận (OTC deals) không ảnh hưởng trực tiếp lên Order Book. |

Một kịch bản điển hình của "Bull Run" là khi `whale_tx_count` tăng cao nhưng chủ yếu là Outflow, cho thấy cá voi đang gom hàng quyết liệt. 
Ngược lại, nếu `whale_volume` Inflow tăng vọt khi giá đang ở đỉnh, đó là tín hiệu phân phối (xả hàng) rõ ràng. 

## 5. Tổng hợp chiến lược - Ma trận tín hiệu (Signal Matrix)

| Tín hiệu Chỉ số | Diễn giải Trạng thái Thị trường | Hành động Khuyến nghị |
|---|---|---|
| **Vol Up + Price Up + Momentum Up** | **Strong Bullish**: Xu hướng tăng mạnh, được hỗ trợ bởi dòng tiền và sự đồng thuận. | Canh mua tại các nhịp điều chỉnh nhỏ (Dip). |
| **Vol Up + Price Down + Momentum Down** | **Strong Bearish**: Bán tháo hoảng loạn (Panic Selling). Áp lực bán cực mạnh. | Tránh bắt đáy ("Don't catch a falling knife"). Tìm kiếm vị thế Short. |
| **Vol Down + Price Up** | **Weak Bullish**: Tăng giá nhưng thiếu khối lượng (Divergence). Dễ đảo chiều. | Cẩn trọng, chốt lời từng phần. Không mua đuổi. |
| **Whale Inflow High + Price High** | **Distribution Warning**: Cá voi chuyển tiền lên sàn khi giá đang cao. | Cảnh báo xả hàng. Đặt Stop Loss chặt chẽ. |
| **Vol Spike Ratio > 5.0 + Price Flat** | **Accumulation/Churning**: Gom hàng hoặc Trao tay. Chuẩn bị có biến động lớn. | Quan sát hướng phá vỡ (Breakout direction). |

