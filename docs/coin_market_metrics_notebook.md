# Phương pháp phân tích định lượng thị trường Crypto

## 1. Phân tích định giá: Vốn hoá thị trường và giá trị thực

Một trong những ngộ nhận phổ biến nhất của nhiều người mới tham gia thị trường crypto
là tư duy "giá rẻ" đồng nghĩa với "cơ hội tăng trưởng lớn". Họ thường so sánh giá của một đồng
coin có giá trị \$0.001 với Bitcoin (\$60,000) và kỳ vọng đồng coin nhỏ đó sẽ tăng lên $1, Tuy nhiên, 
phân tích chuyên sâu về market_cap sẽ chỉ ra tính bất khả thi của kfy vọng này dựa trên nguyên tắc kinh tế học cơ bản. 

### 1.1. Bản chất kinh tế của vốn hoá thị trường 

**Vốn hoá thị trường (`Market Cap`)** là chỉ số đo lường quy mô tương đối của một tài sản tiền mã hoá, được tính bằng tích số giữa giá hiện tại (`Current Price`) và lượng cung đang lưu thông (`Circulating Supply`). 

$$Market\ Cap = Current\ Price \times Circulating\ Supply$$

Khác với thị trường chứng khoán, nơi vốn hoá phản ánh vốn chủ sở hữu và hiệu suất kinh doanh (doanh thu, lợi nhuận), **vốn hoá trong crypto chủ yếu được thúc đẩy bởi tâm lý thị trường, quy luật cung cầu, kỳ vọng về mạng lưới.** 

Điều quan trọng cần lưu ý là con số `Market Cap` không đại diện cho lượng tiền thực tế (fiat) đã chảy vào tài sản đó. Nó chỉ là một giá trị
lý thuyết phản ánh giá trị của toàn bộ lượng coin đang lưu hành nếu chúng được bán ngay lập tức tại mức giá hiện tại. Tuy nhiên, trong thực tế, 
nếu một lượng lớn coin được bán ra cùng lúc, giá sẽ sập (slippage) và vốn hoá thực tế thu về thấp hơn nhiều so với con số trên bảng điện tử. 

Các nhà đầu tư chuyên nghiệp sử dụng chỉ số `Market Cap` để phân loại rủi ro và xác định vị thế của tài sản trong chu kỳ kinh tế: 

| Phân Loại Vốn Hóa | Quy Mô (USD) | Đặc Điểm Nhận Diện | Hồ Sơ Rủi Ro |
|-------------------|--------------|-------------------|--------------|
| **Large-Cap (Vốn hóa lớn)** | > 10 tỷ USD | Tính thanh khoản cao, biến động thấp hơn so với thị trường chung. Thường là các nền tảng hạ tầng (Bitcoin, Ethereum). | Thấp - Trung bình. Phù hợp nắm giữ dài hạn. |
| **Mid-Cap (Vốn hóa trung bình)** | 1 tỷ - 10 tỷ USD | Đang trong giai đoạn mở rộng thị phần. Có tiềm năng tăng trưởng mạnh nhưng đi kèm rủi ro cạnh tranh cao. | Trung bình - Cao. Cân bằng giữa rủi ro và lợi nhuận. |
| **Small-Cap (Vốn hóa nhỏ)** | < 1 tỷ USD | Biến động cực mạnh, dễ bị thao túng giá bởi "cá voi". Tiềm năng lợi nhuận đột biến nhưng rủi ro mất trắng cao. | Rất cao. Phù hợp với chiến lược đầu cơ ngắn hạn. |

Sự phân loại này giúp nhà đầu tư không bị đánh lừa bởi mệnh giá thấp. Một đồng coin giá \$0.50 nhưng có vốn hoá 50 tỷ USD (do nguồn cung 100 tỷ coin) sẽ khó tăng gấp đôi hơn nhiều so với đồng coin $100 nhưng vốn hoá chỉ 100 triệu USD. 

**Tham khảo:**
* https://www.investopedia.com/crypto-cheat-sheet-decoding-market-cap-volume-and-trends-11833687

### 1.2. Định giá pha loãng hoàn toàn (FDV): Cái bẫy của sự lạm phát

Trong khi `Market Cap` cho biết giá trị hiện tại, một chỉ số nâng cao hơn là **Fully Diluted Valuation (FDV)**. Đây là giá trị vốn hoá thị trường giả định nếu toàn bộ nguồn cung tối đa (Max Supply hoặc Total Supply) đều được đưa vào lưu thông. 

Công thức tính toán: 

$$ FDV = Current\ Price \times Max\ Supply$$

Sự chênh lệch giữa Market Cap và FDV là chỉ báo quan trọng về áp lực lạm phát trong tương lai. Nếu một dự án có Market Cap là 10 triệu USD nhưng FDV lên tới 1 tỷ USD, 
điều này có nghĩa là chỉ mới có 1% lượng token được lưu hành. 99% lượng token còn lại đang bị khoá và được giải phóng trong tương lai thông qua các đợt mở khoá (unlock)
hoặc trả thường staking. Khi lượng cung khổng lồ này tràn ra khỏi thị trường, nếu nhu cầu mua không tăng trưởng tương ứng gấp 100 lần, giá trị token chắc chắn bị pha loãng, 
dẫn đến sự sụt giảm giá mạnh mẽ. 

Nghiên cứu từ các trường hợp lịch sử cho thấy các dự án có tỷ lệ Market Cap/FDV thấp thường đối mặt với áp lực bán dài hạn. Ví dụ, token TryHards (TRY) đã mất phần lớn giá trị ngay sau khi lượng cung bổ sung được giải phóng ra thị trường.1 Do đó, FDV không cho biết giá trị hiện tại, nhưng nó cảnh báo về giới hạn trần (ceiling) của sự tăng trưởng và rủi ro lạm phát


## 2. Phân tích thanh khoản: Giải mã khối lượng giao dịch 

Nếu vốn hoá thị trường là "cơ thể" của một con số, thì khối lượng giao dịch (`Total Volume`) chính là "dòng máu" duy trì sự sống của cơ thể đó. 

`Total Volume` là chỉ số đại diện cho tổng giá trị giao dịch trong 24 giờ qua. Đây là thước đo trực tiếp nhất về tính thanh khoản và sự quan tâm của thị trường đối với tài sản. 

### 2.1. Mối tương quan giữa Volume và tính xác thực của xu hướng

Khối lượng giao dịch đóng vai trò là công cụ xác nhận (confirmation tool) cho hành động giá. Một xu hướng tăng giá chỉ được coi là bền vững (sustainable) khi nó đi kèm với sự gia tăng của khối lượng giao dịch. 
Điều này cho thấy đà tăng được hỗ trợ bởi dòng tiền thực sự tham gia vào thị trường. Ngược lại, nếu giá tăng nhưng khối lượng giao dịch giảm dần (phân kỳ âm), đây là tín hiệu cảnh báo về sự suy yếu của lực mua và khả
năng đảo chiều xảy ra, thường được gọi là hiện tượng "kiệt sức" (exhaution). 


Trong chiều hướng ngược lại, khi giá giảm mạnh kèm theo khối lượng giao dịch tăng đột biến, điều này phản ánh tâm lý hoảng loạn (panic sell) của đám đông. 
Tuy nhiên, đây cũng có thể là đấu hiệu của việc "rũ bỏ" (shakeout), nơi các nhà đầu tư yếu tay bán tháo tài sản cho các nhà đầu tư dài hạn, tạo tiền đề cho sự phục hồi sau đó. 


### 2.2. Tỷ lệ Volume/Market Cap (Vol/MC Ratio)

Để đánh giá mức độ thanh khoản một cách khách quan, không thể nhìn vào con số volume tuyệt đối mà cần so sánh nó với quy mô vốn hoá. 
Tỷ lệ Volume/Market Cap là một chỉ số phái sinh quan trọng giúp nhà đầu tư nhận diện trạng thái hoạt động của tài sản. 

Công thức: 

$$\text{Vol/MC Ratio} = \frac{\text{24h Total Volume}}{\text{Market Cap}}$$

Dựa trên dữ liệu thị trường, tỷ lệ này có thể diễn giải như sau: 

* **Ratio > 0.1 (Thanh khoản cao):** Cho thấy tài sản đang được giao dịch sôi động, dễ dàng mua bán mà không gây trượt giá lớn. 
Đây thường là đặc điểm của tài sản Large-cap hoặc các đồng coin đang trong xu hướng tăng mạnh (hot trend).

* **Ratio < 0.01 (Thanh khoản thấp):** Cảnh báo về các "zombie-chain" hoặc dự án bị lãng quên. Việc nắm giữ các tài sản này mang lại rủi ro thanh khoản lớn, nhà đầu tư có thể không thể thoát hàng khi thị trường biến động.

* **Dấu hiệu bất thường:** Trong trường hợp các đồng coin vốn hoá nhỏ có tỷ lệ này vượt quá 1.0 (Volume lớn hơn cả vốn hoá), nhà đầu tư cần cảnh giác với khả năng thao túng giá (wash trading) hoặc các hành vi làm giả khối lượng để thu hút sự chú ý. 

Tham khảo:
* https://calebandbrown.com/blog/crypto-liquidity/


### 2.3. Thanh khoản thực tế (Liquidity) so với khối lượng (Volume)

Mặc dù Total Volume là chỉ số quan trọng, nhưng nó không hoàn toàn đồng nghĩa với Thanh khoản (Liquidity). 
Volume chỉ cho biết số lượng giao dịch đã thực hiện, trong khi thanh khoản thực sự (Market Depth) phản ảnh khả năng 
hấp thụ các lệnh mua/bán lớn tại các mức giá gần nhất. 

Một tài sản có thể có volume cao do các bot giao dịch tần suất cao (HFT) thực hiện, nhưng sổ lệnh (order book) lại rất mỏng (thin liquidity). 
Điều này dẫn đến việc giá có thể biến động dữ dội (slippage cao) khi có một lệnh thị trường lớn xuất hiện. Do đó, việc quan sát thêm chênh lệch giá mua-bán (bid-ask spread) là cần thiết để đánh giá chất lượng thanh khoản thực sự.

## 3. Cấu trúc Tokenomics: Phân tích các loại Supply

Sự khác biệt giữa Circulating Supply, Total Supply, Max Supply quyết định áp lực lạm phát và tiềm năng tăng giá dài hạn. 

### 3.1. Phân định các loại nguồn cung
| Loại Nguồn Cung | Định Nghĩa Chi Tiết | Ý Nghĩa Trong Phân Tích Đầu Tư |
|-----------------|---------------------|--------------------------------|
| **Circulating Supply (Cung lưu thông)** | Số lượng token đang thực sự lưu hành trên thị trường và công chúng có thể tự do giao dịch. Không bao gồm các token bị khóa, token của quỹ dự trữ, hoặc token chưa được phân phối. | Đây là biến số chính để tính toán `market_cap`. Sự khan hiếm của cung lưu thông thường là yếu tố ngắn hạn thúc đẩy giá tăng khi nhu cầu vượt quá cung. |
| **Total Supply (Tổng cung)** | Tổng số lượng token đã được tạo ra (minted) trên blockchain, bao gồm cả số đang lưu thông và số đang bị khóa (locked/vesting). Con số này trừ đi số token đã bị đốt (burned) vĩnh viễn. | Đại diện cho tổng tài sản hiện hữu của dự án. Khoảng chênh lệch giữa Total và Circulating Supply chính là "nguồn cung treo" (supply overhang) có thể gây áp lực bán trong tương lai. |
| **Max Supply (Cung tối đa)** | Giới hạn cứng về số lượng token tối đa có thể tồn tại theo thiết kế của giao thức (ví dụ: Bitcoin là 21 triệu). Không thể tạo thêm token vượt quá con số này. | Xác định tính khan hiếm tuyệt đối. Các dự án có Max Supply cố định thường được coi là công cụ lưu trữ giá trị chống lạm phát tốt hơn (Deflationary potential). |

### 3.2. Lạm phát (Inflationary) và Giảm phát (Deflationary)

Từ dữ liệu Supply, nhà đầu tư có thể xác định mô hình kinh tế vĩ mô của token, yếu tố quyết định giá trị dài hạn:
* **Mô Hình Giảm Phát (Deflationary):** Điển hình là Bitcoin (với cơ chế Halving giảm nguồn cung mới) hoặc BNB (với cơ chế đốt coin định kỳ). Khi nguồn cung giới hạn hoặc giảm dần theo thời gian trong khi nhu cầu giữ nguyên hoặc tăng lên, giá trị của mỗi đơn vị token có xu hướng tăng theo nguyên lý khan hiếm. Bitcoin được thiết kế như một tài sản giảm phát để chống lại sự mất giá của tiền pháp định.   
* **Mô Hình Lạm Phát (Inflationary):** Điển hình là Dogecoin hoặc các token trả thưởng Farming (Yield Farming). Các dự án này không có max_supply hoặc có tỷ lệ phát hành thêm (emission rate) rất cao để khuyến khích người dùng tham gia mạng lưới. Tuy nhiên, nếu tốc độ lạm phát (nguồn cung mới) cao hơn tốc độ tăng trưởng người dùng (nhu cầu mới), giá trị token sẽ bị suy giảm theo thời gian. Ethereum trước đây là lạm phát, nhưng sau nâng cấp EIP-1559 với cơ chế đốt phí, nó đã chuyển sang mô hình giảm phát trong những thời điểm mạng lưới hoạt động mạnh.

### 3.3. Tác động của Token Unlocks (Mở khoá token)

Một yếu tố ngoại sinh quan trọng liên quan đến supply là lịch trình mở khóa token (Token Unlocks). Đây là sự kiện khi các token bị khóa (thuộc về đội ngũ phát triển, quỹ đầu tư mạo hiểm - VC) được giải phóng vào thị trường.

Có hai hình thức mở khóa chính:

* **Cliff Unlock:** Mở khóa một lượng lớn token tại một thời điểm cụ thể. Sự kiện này thường tạo ra áp lực bán mạnh và tâm lý lo ngại trước ngày mở khóa, khiến giá thường giảm trước khi sự kiện diễn ra.   

* **Linear Unlock:** Mở khóa tuyến tính, nhỏ giọt theo thời gian (ví dụ: hàng giây hoặc hàng ngày). Mặc dù ít gây sốc giá (price shock) hơn Cliff Unlock, nhưng nó tạo ra một áp lực lạm phát dai dẳng (sell pressure) lên giá token.   

Dữ liệu thực tế cho thấy không phải mọi đợt unlock đều khiến giá giảm; nếu thị trường đang trong xu hướng tăng mạnh (uptrend) và lực mua đủ lớn để hấp thụ nguồn cung mới, giá vẫn có thể tăng (như trường hợp của Optimism (OP) trong một số đợt unlock). Tuy nhiên, đối với người mới, việc theo dõi lịch unlock là bắt buộc để tránh mua vào ngay trước các đợt xả hàng tiềm năng.   


## 4. Phân tích hành vi giá: Ý nghĩa của ATH, ATL và biến động

Các chỉ số lịch sử như **ATH** (All-Time High) và **ATL** (All-Time Low) không chỉ là những con số thông kê quá khứ, mà chúng đóng vai trò là các mốc tâm lý quan trọng định hình hành vi của nhà đầu tư hiện tại. 

### 4.1. Tâm lý học đằng sau ATH và ATL
* **ATH (Đỉnh lịch sử):** Mức giá cao nhất từng được ghi nhận. Khi giá của một tài sản tiếp cận lại vùng ATH cũ, nó thường gặp phải "kháng cự tâm lý". 
Nguyên nhân là do những nhà đầu tư đã mua ở đỉnh trước đó (đu đỉnh) và chịu lỗ trong thời gian dài sẽ có xu hướng bán ra để hoà vốn (break-even) khi giá quay lại mức này. 
Tuy nhiên, nếu phá vỡ (breakout) thành công qua mức ATH, tài sản sẽ bước vào giai đoạn "khám phá giá" (price discovery) - nơi không còn mức kháng cự này ở phía trên, thường dẫn đến đà tăng mạnh mẽ do hiệu ứng FOMO. 

* **ATL (Đáy lịch sử):** Mức giá thấp nhất từng được ghi nhận. Đây thường được coi là vùng hỗ trợ cứng về mặt tâm lý. Tuy nhiên, việc bắt đáy tại ATL chứa đựng rủi ro cao vì trong một xu hướng giảm mạnh, giá hoàn toàn có thể phá vỡ ATL để thiết lập các đấy mới sâu hơn. 

### 4.2. Chỉ số Drawdown và tiềm năng phục hồi

Sử dụng Current Price và ATH, nhà đầu tư có thể tính toán mức độ sụt giảm (Drawdown) từ đỉnh:

$$\text{Drawdown} = \frac{\text{Current Price} - \text{ATH}}{\text{ATH}} \times 100\%$$

Chỉ số này giúp đánh giá rủi ro và cơ hội. Một tải sản đã giảm 90% từ đỉnh (chia 10) cần phải tăng trưởng 900% (nhân 10) mới có thể quay lại mức giá cũ. 
Điều này minh hoạ cho sự khó khăn của việc phục hồi sau những cú sập giá mạnh. Đối với các đồng coin có nền tảng tốt, mức Drawdown sâu có thể là cơ hội mua vào,
nhưng đối với dự án yếu kém, đó có thể là dấu hiệu của sự sụp đổ hoàn toàn. 

### 4.3. Phân tích biến động giá theo khung thời gian (Change Percentage)
Dữ liệu biến động giá thường được phân chia theo các khung thời gian 1h, 24h, và 7d, mỗi khung mang một ý nghĩa phân tích riêng biệt:
* **1h %:** Phản ánh biến động tức thời và tâm lý ngắn hạn (short-term sentiment). Thường chứa nhiễu (noise) và phù hợp cho các nhà giao dịch lướt sóng (scalpers).
* **24h %:** Chu kỳ giao dịch tiêu chuẩn toàn cầu. Dùng để xác định xu hướng trong ngày và so sánh sức mạnh tương đối của đồng coin so với thị trường chung. 
* **7d %:** Chỉ báo quan trọng về xu hướng trung hạn. Nếu `24 %` giảm nhưng `7d %` vẫn tăng, điều đó cho thấy thị trường chỉ đang trong nhịp điều chỉnh ngắn hạn (correction) trong một xu hướng tăng giá lớn hơn. Ngược lại, nếu cả hai chỉ số đều giảm, xu hướng giảm (downtrend) đang được củng cố.

Ngoài ra, việc quan sát biên độ giao động (`24_high` và `24_low`) giúp xác định độ biến động (volatility) trong ngày. Biến động càng rộng, rủi ro càng cao nhưng cơ hội lợi nhuận ngắn hạn càng lớn. 
Chiến lược giao dịch trong biên độ (Range Trading) thường dựa vào việc mua ở gần `24_high` khi thị trường đi ngang.