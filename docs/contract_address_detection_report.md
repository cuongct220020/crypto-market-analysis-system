# Phân tích Smart Conatract trên Ethereum

## Mục lục
  * [1. Giới thiệu tổng quan và phạm vi nghiên cứu](#1-giới-thiệu-tổng-quan-và-phạm-vi-nghiên-cứu)
  * [2. Phân tích và định danh tiêu chuẩn token (Token Standards)](#2-phân-tích-và-định-danh-tiêu-chuẩn-token-token-standards)
    * [2.1. Cơ chế Intropection và vai trò ERC-165](#21-cơ-chế-intropection-và-vai-trò-erc-165)
    * [2.2. Chiến lược định danh ERC-20: Từ Heuristic đến ByteCode](#22-chiến-lược-định-danh-erc-20-từ-heuristic-đến-bytecode)
      * [2.2.1. Phương pháp phân tích Function Selectors](#221-phương-pháp-phân-tích-function-selectors)
      * [2.2.2. Kỹ thuật trích xuất Selector từ Bytecode (Static Analysis)](#222-kỹ-thuật-trích-xuất-selector-từ-bytecode-static-analysis)
    * [2.3. Định danh ERC-721: Non-Fungible Tokens (NFT)](#23-định-danh-erc-721-non-fungible-tokens-nft)
    * [2.4. Định danh ERC-1155: Multi-Token Standard](#24-định-danh-erc-1155-multi-token-standard)
  * [3. Kiến trúc Proxy: Phân tích và nhận diện mô hình nâng cấp](#3-kiến-trúc-proxy-phân-tích-và-nhận-diện-mô-hình-nâng-cấp)
    * [3.1. EIP-1167 Minimal Proxy (Clones) - Sự tối ưu hoá cực đoan](#31-eip-1167-minimal-proxy-clones---sự-tối-ưu-hoá-cực-đoan)
    * [3.2. EIP-1967: Chuẩn hoá Storage Slots cho Proxy](#32-eip-1967-chuẩn-hoá-storage-slots-cho-proxy)
    * [3.3. Phân biệt Transparent Proxy và UUPS](#33-phân-biệt-transparent-proxy-và-uups)
    * [3.4. Diamond Proxy (EIP-2535): Da diện và Linh hoạt](#34-diamond-proxy-eip-2535-da-diện-và-linh-hoạt)
    * [3.5. Gnosis Safe Proxy: Chuẩn mực của Multisig](#35-gnosis-safe-proxy-chuẩn-mực-của-multisig)
  * [4. Phân loại danh mục (Category Classification) - Dựa trên chữ ký hành vi](#4-phân-loại-danh-mục-category-classification---dựa-trên-chữ-ký-hành-vi)
    * [4.1. Sàn giao dịch phi tập trung (DEX) & AMM](#41-sàn-giao-dịch-phi-tập-trung-dex--amm)
    * [4.2. Oracle Networks (Chainlink)](#42-oracle-networks-chainlink)
    * [4.3. Cross-chain Bridges (LayerZero & Wormhole)](#43-cross-chain-bridges-layerzero--wormhole)
  * [5. Phân tích vòng đời Contract (Lifecycle Analysis) (Optional)](#5-phân-tích-vòng-đời-contract-lifecycle-analysis-optional)
    * [5.1. Thuật Toán Binary Search Tìm Creation Block](#51-thuật-toán-binary-search-tìm-creation-block)
    * [5.2. Truy Vết Creator Address và Transaction Trace](#52-truy-vết-creator-address-và-transaction-trace)
  * [6. Tổng hợp kiến trúc hệ thống tự động hoá](#6-tổng-hợp-kiến-trúc-hệ-thống-tự-động-hoá)


## 1. Giới thiệu tổng quan và phạm vi nghiên cứu

Trong bối cảnh hệ sinh thái Ethereum và các blockchain tương thích với máy ảo Ethereum (EVM) đang mở rộng với tốc độ chóng mặt, số lượng hợp đồng 
thông minh (smart contract) được triển khai hàng ngày đã đạt đến con số khổng lồ. Đối với các nhà phát triển ứng dụng phi tập trung (dApps), các nhà phân tích dữ liệu on-chain, 
và các đơn vị cung cấp dịch vụ thám hiểm khối (block explorer), việc tự động hóa quá trình định danh và phân loại các hợp đồng này không còn là một lựa chọn bổ sung mà là một yêu cầu cốt lõi. 
Mô hình dữ liệu EthContract yêu cầu sự chính xác tuyệt đối trong việc điền các trường thông tin quan trọng: Tiêu chuẩn Token (Token Standard), Mô hình Proxy (Proxy Pattern), Phân loại Danh mục (Category), và Vòng đời Hợp đồng (Lifecycle).

Thách thức lớn nhất nằm ở tính chất "mờ đục" của bytecode EVM. Khi một hợp đồng được biên dịch và triển khai lên mạng lưới,
các thông tin ngữ nghĩa cấp cao (như tên biến, cấu trúc hàm, hoặc tài liệu hướng dẫn) bị loại bỏ, chỉ còn lại chuỗi mã máy (opcodes).
Báo cáo này sẽ cung cấp một phân tích toàn diện, đi sâu vào các kỹ thuật dịch ngược (reverse engineering), phân tích tĩnh (static analysis),
và các heuristic (quy tắc kinh nghiệm) để trích xuất thông tin định danh từ địa chỉ contract một cách tự động và chính xác. 
Chúng ta sẽ không dừng lại ở việc liệt kê các phương pháp, mà sẽ phân tích sâu cơ chế hoạt động, lịch sử hình thành, và các rủi ro kỹ thuật tiềm ẩn của từng phương pháp. 


## 2. Phân tích và định danh tiêu chuẩn token (Token Standards)

Việc xác định một smart contract tuân theo tiêu chuẩn token nào là bước đầu tiên và quan trong nhất trong việc định danh tài sản số. 
Các tiêu chuẩn ERC (Ethereum Request for Comments) định hình cách thức tương tác giữa các ứng dụng và tài sản, 
nhưng sự thiếu đồng nhất trong lịch sử triển khai đã tạo ra những thách thức đáng kể cho việc phát hiện tự động. 

### 2.1. Cơ chế Intropection và vai trò ERC-165

Để giải quyết vấn đề "nhận dạng" giao diện của một contract, cộng đồng Ethereum đã giới thiệu ERC-165 (Standard Interface Detection).
Đây là nền tnarg kỹ thuật cho phép một smart contract tự tuyên bố các giao diện (interfaces) mà nó hỗ trợ thông qua hàm `supportsInterface(bytes4 interfaceId)`.


Cơ chế hoạt động của ERC-165 dựa trên việc tính toán `interfaceId`. `interfaceId` của một giao diện được định nghĩa là kết quả của phép toán XOR tất các các bộ chon hàm
(function selectors) có trong giao diện đó. Ví dụ, nếu một giao diện có hai hàm `functionA` và `functionB`, thì `interfaceId` sẽ là `bytes4(keccak256('functionA()')) ^ bytes4(keccak256('functionB()'))`.

Khi hệ thống phân tích muốn kiểm tra xem một contract có hỗ trợ một tiêu chuẩn cụ thể hay không, nó sẽ thực hiện một cuộc gọi (static call) đến hàm `supportsInterface` với `interfaceId` mong muốn.

* Nếu contract trả về `true` (và cũng trả về `false` khi hỏi về `0xffffffff` theo quy định của chuẩn để tránh false positive), thì contract đó được xác nhận là hỗ trợ tiêu chuẩn.
* Tuy nhiên, điểm yêu chí mạng của phương pháp này là nó phụ thuộc vào việc nhà phát triển contract có chủ động thực thi ERC-165 hay không. Rất nhiều contract cũ, đặc biệt là các ERC-20 token đời đầu, không hỗ trợ cơ chế này. 


**Tham khảo:**
* https://www.reddit.com/r/ethdev/comments/10rniuv/given_an_sc_address_how_to_check_if_its_erc20/
* https://stackoverflow.com/questions/45364197/how-to-detect-if-an-ethereum-address-is-an-erc20-token-contract/53352634#53352634


### 2.2. Chiến lược định danh ERC-20: Từ Heuristic đến ByteCode

ERC-20 là tiêu chuẩn token phổ biến nhất cho các tài sản thay thế được (fungible tokens), nhưng nghịch lý thay, nó lại tiêu chuẩn khó xác định tự động nhất theo cách chuẩn hoá.
Do phần lớn các triển khai ERC-20 không hỗ trợ ERC-165, việc phát hiện phải dựa trên phương pháp "Duck Typing" - kiểm tra xem contract có các đặc điểm hành vi giống ERC-20 hay không. 


#### 2.2.1. Phương pháp phân tích Function Selectors

Mọi hàm trong Ethereum đều được định danh bởi 4 byte đầu tiên của mã băm Keccak-256 của chữ ký hàm (function signature). 
Để xác định ERC-20, hệ thống cần quét bytecode hoặc thực hiện giả lập cuộc gọi (static call) để tìm sự hiện diện của các selector bắt buộc:

| Tên hàm | Chữ ký hàm (Signature) | Function Selector (4-byte Hex) | Ý nghĩa kỹ thuật |
|----------------|---------------------------------------------|--------------------------------|----------------------------------|
| totalSupply | `totalSupply()` | `0x18160ddd` | Trả về tổng cung của token |
| balanceOf | `balanceOf(address)` | `0x70a08231` | Truy vấn số dư của một địa chỉ |
| transfer | `transfer(address,uint256)` | `0xa9059cbb` | Chuyển token từ người gọi đến người nhận |
| transferFrom | `transferFrom(address,address,uint256)` | `0x23b872dd` | Chuyển token thay mặt người khác (ủy quyền) |
| approve | `approve(address,uint256)` | `0x095ea7b3` | Cấp quyền chi tiêu token |
| allowance | `allowance(address,address)` | `0xdd62ed3e` | Kiểm tra hạn mức chi tiêu còn lại |

Một chiến lược heuristic phổ biến nhưng hiệu quả ("quick and dirty") là thực hiện gọi thử (dry-run) hàm `balanceOf(address)` với một địa chỉ bất kỳ (thường là địa chỉ 0 hoặc chính là địa chỉ contract).
Nếu contract phản hồi mọt giá trị số nguyên 32 bytes mà không bị revert, xác suất cao đó là một contract ERC-20. Tuy nhiên, để đảm bảo độ chính xác tuyệt đối và tránh các trường hợp dương tính giả (false positive)
(ví dụ: một contract không phải token nhưng tình cờ có hàm `balanceOf`), hệ thống kiểm tra sự tồn tại của đầy đủ bộ 6 hàm trên trong bytecode. 

**Tham khảo:**
* https://stackoverflow.com/questions/45364197/how-to-detect-if-an-ethereum-address-is-an-erc20-token-contract/53352634#53352634


#### 2.2.2. Kỹ thuật trích xuất Selector từ Bytecode (Static Analysis)

Thay vì thực hiện các cuộc gọi RPC tốn kém và chậm chạp, phương pháp tối ưu hơn là phân tích tĩnh bytecode của contract. Trình biên dịch Solidity tạo ra một cấu trúc điều hướng (dispatcher) ở đầu bytecode runtime để định 
tuyến các cuộc gọi hàm. Cấu trúc này thường tuân theo mẫu opcode:
1. **DUP1:** Sao chép dữ liệu từ stack (thường là selector từ calldata).
2. **PUSH4:** Đẩy 4 byte selector cần so sánh vào stack. 
3. **EQ:** So sánh selector đầu vào với selector đã push.
4. **PUSH2:** Đầy địa chỉ nhảy (jump destination) vào stack.
5. **JUMPI:** Nhảy đến đoạn code xử lý hàm nếu so sánh đúng. 

Bằng cách quyét các chuỗi opcode này, ta có thể trích xuất toàn bộ danh sách các hàm mà contract hỗ trợ mà không cần tương tác với mạng lưới blockchain. 
Các công cụ như `evmhole` hay `WhatsABI` sử dụng thuật toán này để tái tạo giao diện contract ngay cả khi không có mã nguồn. 

**Tham khảo:**
* https://github.com/cdump/evmole
* https://www.synacktiv.com/publications/evm-unravelled-recovering-abi-from-bytecode


### 2.3. Định danh ERC-721: Non-Fungible Tokens (NFT)

Đối với ERC-721, cộng đồng Ethereum đã rút kinh nghiệm từ sự hỗn loạn của ERC-20 và bắt buộc hỗ trợ ERC-165 ngay từ trong đặc tả kỹ thuật (EIP Specification).
Điều này làm cho việc định danh ERC-721 trở nên minh bạch và chuẩn xác hơn nhiều. 
* **Interface ID chính:** `0x80ac58cd` Đây là kết quả XOR của các hàm cơ bản như `balanceOf`, `ownerOf`, `approve`, `getApproved`, `setApprovalForAll`, `transferFrom` và `safeTransferFrom`.
* **Quy trình kiểm tra:**
1. Gọi `supportsInterface(0x80ac58cd)`. Nếu trả về `true`, contract chắc chắn là ERC-721.
2. Kiểm tra thêm Interface ID của `ERC721Metadata` (`0x5b5e139f`) để xác định xem contract có hỗ trợ `name()`, `symbol()`, và `tokenURI()` hay không. 
3. Kiểm tra Interface ID của `ERC721Enumerable` (`0x780e9d63`) nếu cần thông tin về khả năng liệt kê toàn bộ token. 

Sự kiện (Event) cũng đóng vai trò quan trọng trong việc xác nhận. ERC-721 bắt buộc phải emit sự kiện `Transfer(address indexed from, address indexed to, uint2536 indexed tokenId)`. 
Lưu ý rằng sự kiện này có 3 tham số được đánh chỉ mục (indexed), khác với sự kiện `Transfer` của ERC-20 chỉ có 2 chỉ mục (Value không được index). 
Sự khác biệt nhỏ này trong signature của Event Topic là một dấu hiệu nhận biết cực kỳ hữu ích khi phân tích log giao dịch. 

**Tham khảo:**
* https://docs.metamask.io/services/tutorials/ethereum/retrieve-and-display-erc-721-and-erc-1155-tokens
* https://www.quicknode.com/guides/ethereum-development/transactions/how-to-audit-token-activity-using-quicknode-sdk

### 2.4. Định danh ERC-1155: Multi-Token Standard

ERC-1155 đại diện cho sự tiến hoá tiếp theo, cho phép quản lý cả token thay thế được (fungible token) và không thay thế được (non-fungible token) trong cùng một hợp đồng, tối ưu hoá gas thông qua các thao tác lô (batch operations).
Tương tự ERC-721, ERC-1155 cũng bắt buộc hỗ trợ ERC-165. 
* **Interface ID chính:** `0xd9b67a26`
* **Các hàm đặc trưng:** `safeTransferFrom`, `safeBatchTransferFrom`, `balanceOfBatch`.
* **Sự kiện đặc trưng:** `TransferSingle` và `TransferBatch`. Sự xuất hiện của sự kiện này trong log giao dịch là bằng chứng đanh thép cho thấy địa chỉ dó là một contract ERC-1155.

Bảng tổng hợp các Interface ID quan trọng cho việc định danh Token Standard: 

| Tiêu chuẩn Token | Interface ID (Hex) | Tính chất bắt buộc ERC-165 | Mức độ tin cậy |
|-------------------|--------------------|-----------------------------|-----------------|
| ERC-165 | `0x01ffc9a7` | N/A | Cao (Base check) |
| ERC-721 | `0x80ac58cd` | Có | Rất Cao |
| ERC-1155 | `0xd9b67a26` | Có | Rất Cao |
| ERC-721 Metadata | `0x5b5e139f` | Không (Optional) | Trung bình |
| ERC-721 Enumerable | `0x780e9d63` | Không (Optional) | Trung bình |

**Tham khảo:**
* https://speedrunethereum.com/guides/erc721-vs-erc1155
* https://docs.openzeppelin.com/contracts/3.x/erc1155

## 3. Kiến trúc Proxy: Phân tích và nhận diện mô hình nâng cấp

Trong phát triển Ethereum, tính bất biến (immutability) của mã bytecode vừa là tính năng bảo mật, vừa là rào cản cho việc sửa lỗi và nâng cấp. 
Để giải quyết vấn đề này, các mô hình Proxy (Proxy Patterns) ra đời, tách biệt logic thực thi (Implementation) khỏi trạng thái dữ liệu (Storage).
Việc xác định chính xác loại Proxy là cực kỳ quan trọng để hệ thống có thể truy xuất đúng địa chỉ implementation, từ đó mới có thể phân tích đúng logic nghiệm vụ của contract. 


### 3.1. EIP-1167 Minimal Proxy (Clones) - Sự tối ưu hoá cực đoan

EIP-1167 là tiêu chuẩn proxy tối giản nhất, được thiết kế để giải quyết chi phí gas khi cần triển khai nhiều bản sao của cùng một logic contract
(ví dụ: một người dùng có một ví Multisig riêng). Thay vì triển khai toàn bộ bytecode logic nhiều lần, EIP-1167 triển khai một đoạn bytecode cực ngắn (khoảng 55 bytes) chỉ làm nhiệm vụ chuyển tiếp cuộc gọi.
* **Cấu trúc Bytecode:** Minimal Proxy không có logic quản trị, không có biến trạng thái (ngoài địa chỉ implementation được hardcode trong bytecode).
Nó sử dụng opcocde `DELEGATECALL` để mượn logic từ contract mẫu. 
* **Nhận diện mẫu (Pattern Matching):** Bytecode của EIP-1167 có cấu trúc cố định và đặc trưng. Mẫu Hex: `363d3d373d3d3d363d73` + [20 bytes địa chỉ Implementation] + `5af43d82803e903d91602b57fd5bf3`.
  * `36`: `CALLDATAASIZE`
  * `3d`: `RETURNDATASIZE`
  * `37`: `CALLDATACOPY`
  * `73`: `PUSH20` (đẩy địa chỉ implementation)
  * `5a`: `GAS`
  * `f4`: `DELEGATECALL`
* **Phương pháp trích xuất:** Hệ thống chỉ cần so sánh chuỗi hex của bytecode runtime với mẫu trên. Nếu khớp, 20 bytes nằm ở giữa chính là implementation. 
Đây là phương pháp xác định tất định (deterministic) và có độ chính xác 100%. 

**Tham khảo:**
* https://deadlytechnology.com/blog/blockchain/ethereum-proxy-patterns
* https://rareskills.io/post/eip-1167-minimal-proxy-standard-with-initialization-clone-pattern
* https://medium.com/@andrey_obruchkov/proxies-and-upgradability-minimal-proxy-eip-1167-650869226293


### 3.2. EIP-1967: Chuẩn hoá Storage Slots cho Proxy
Với các Proxy phức tạp hơn có khả năng nâng cấp (Upgradeable Proxies), vấn đề lớn nhất là "xung đột bộ nhớ" (Storage Collision). Nếu Proxy lưu địa chỉ implementation tại slot 0,
và implementation contract cũng dùng slot 0 cho biến `owner`, dữ liệu sẽ bị ghi đè và hỏng. EIP-1967 giải quyết vấn đề này bằng cách quy định các vị trí lưu trữ ngẫu nhiên (dựa trên hash)
nhưng cố định để lưu các biến hệ thống của Proxy, đảm bảo chúng nằm ở vùng bộ nhớ cực xa mà các biến thông thường không bao giờ chạm tới. 

Các Slot chuẩn của EIP-1967:
1. Implementation Slot:
   * Công thức: `bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)`
   * Giá trị Hex: `0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc`
2. Admin Slot:
   * Công thức: `bytes32(uint256(keccak256('eip1967.proxy.admin')) - 1)`
   * Giá trị Hex: `0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103`
3. Beacon Slot:
   * Công thức: `bytes32(uint256(keccak256('eip1967.proxy.beacon')) - 1)`
   * Giá trị Hex: `0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50`

Phương pháp phát hiện: Để kiểm tra một contract có phải là EIP-1967 Proxy hay không, ta sử dụng JSON-RPC `eth_getStorageAt` để đọc giá trị tại `Implementation Slot`. 
Nếu giá trị trả về là một địa chỉ khác 0, contract đó chắc chắn alf một EIP-1967 Proxy. 

**Tham khảo:**
* https://www.ethereum-blockchain-developer.com/advanced-mini-courses/solidity-proxy-pattern-and-upgradable-smart-contracts/eip-1967-standard-proxy-storage-slots
* https://rareskills.io/post/erc1967


### 3.3. Phân biệt Transparent Proxy và UUPS

Dù cùng sử dụng EIP-1967 Storage Slots, Transparent Proxy và UUPS (Universal Upgradeable Proxy Standard) có triết lý thiết kế khác nhau về vị trí đặt logic nâng cấp (`upgradeTo`).

* **Transparent Proxy Pattern:**
  * **Cơ chế:** Logic nâng cấp nằm trong chính Proxy Contract. Proxy chặn tất cả các cuộc gọi từ Admin để xử lý nâng cấp, và chỉ forward cuộc gọi (call) từ người dùng thường sang Implementation.
Điều này tốn gas hơn cho người dùng vì mỗi giao dịch đều phải kiểm tra `msg.sender` xem có phải Admin không. 
  * **Dấu hiệu nhận biết:** Proxy Contract có chứa các function selector của hàm quản trị như `upgradeTo(address)` (`0x3659cfe6`) hoặc `upgradeToAndCall(address, bytes)` (`0x4f1ef286`)

* **UUPS (EIP-1822):**
  * **Cơ chế:** Logic nâng cấp được chuyển vào trong **Implementation Contract** Proxy trở nên rất nhẹ (gần như giống Minimal Proxy về mặt chi phí Gas). Implementation Contract phải kế thừa từ 
`UUPSUpgraeable` để đảm bảo có hàm xử lý nâng cấp, nếu không contract sẽ bị "brick" (không thể nâng cấp được nữa) sau lần deploy đầu tiên. 
  * **Dấu hiệu nhận biết:** Proxy Contract **không** có hàm `upgradeTo`. Thay vào đó, nếu ta quét bytecode của địa chỉ **Implementation**, ta sẽ tìm thấy hàm `upgradeTo` hoặc `proxiableUUID()`.

**Quy trình phân loại tự động:**
1. Đọc EIP-1967 Implementation Slot. Nếu có địa chỉ -> Là Proxy. 
2. Quét selector của Proxy. Nếu thấy `upgradeTo` -> **Transparent Proxy**.
3. Nếu không thấy, quét selector của Implementation. Nếu thấy `upgradeTo` -> **UUPS Proxy.**


**Tham khảo:**
* https://medium.com/coinmonks/transparent-proxy-pattern-uups-d7416916789f
* https://docs.openzeppelin.com/contracts/5.x/api/proxy
* https://doc.confluxnetwork.org/docs/general/build/smart-contracts/gas-optimization/uupsAndTransparentProxy

### 3.4. Diamond Proxy (EIP-2535): Da diện và Linh hoạt

Diamond Proxy phá vỡ giới hạn kích thước contract 24KB của Ethereum bằng cách cho phép một Proxy uỷ quyền tới vô số Implementation Contracts khác nhau, gọi là Facets. Mỗi Facet chịu trách nhiệm cho một nhóm hàm cụ thể. 
* **Dấu hiệu nhận biết:** Diamond Proxy bắt buộc phải thực thi giao diện **DiamondLoupe** để cho phép các công cụ bên ngoài khám phá cấu trúc của nó. 
  * `facets()`: Trả về tất cả các facet và selector. 
  * `facetAddressess()`: Selector `0x52ef6b2c`.
  * `facetAddress(bytes4)`: Selector `0xcdffacc6`
  * `diamondCut(tuple)`: Selector `0x1f931c1c` dùng để thêm/sửa/xóa facet.

* **Lưu trữ:** Diamond thường sử dụng `DiamondStorage` tại các vị trí ngẫu nhiên được tính từ keccak256 của chuỗi định danh (namespaced storage) để tránh xung đột.

**Tham khảo:**
* https://www.ethereum-blockchain-developer.com/advanced-mini-courses/solidity-proxy-pattern-and-upgradable-smart-contracts/eip-2535-diamond-standard
* https://rareskills.io/post/diamond-proxy
* https://safe-edges.medium.com/understanding-the-diamond-proxy-pattern-eip-2535-safe-edges-a6b2fe3c85f3
* https://bitsbyblocks.com/eip-2535-diamond-standard-explained-part-3-understanding-storage-patterns-in-diamonds/

### 3.5. Gnosis Safe Proxy: Chuẩn mực của Multisig

Gnosis Safe (hiện là Safe) sử dụng một mô hình Proxy tối ưu hoá riêng. không sử dụng EIP-1967, Gnosis Safe Proxy lưu địa chỉ Master Copy (Implementation) tại **Storage Slot 0**.
* **Phương pháp phát hiện:**
  1. Đọc `eth_getStorageAt(contractAddress, 0)`.
  2. So sánh địa chỉ thu được với danh sách các "Canonical Master Copies" đã được xác minh của Gnosis Safe (ví dụ: `0xd9Db27...`, `0x3E5c63...`). Nếu trùng khớp, đây là một Gnosis Safe Proxy. 
  3. Ngoài ra, có thể kiểm tra sự kiện `ProxyCreation` từ `GnosisSafeProxyFactory` để xác nhận nguồn gốc tạo ra contract. 

**Tham khảo:**
* https://medium.com/@cizeon/gnosis-safe-internals-part-1-safeproxy-3118101e5aed
* https://help.safe.global/en/articles/40834-verify-safe-creation


## 4. Phân loại danh mục (Category Classification) - Dựa trên chữ ký hành vi

Trường `Category` trong mô hình `EthContract` giúp phân loại contract theo chức năng nghiệp vụ (DEX, Lending, Bridge, Oracle,...). 
Do không có tiêu chuẩn ERC cụ thể cho các loại hình này, việc phân loại phải dựa trên "chữ ký hành vi" (behavioral signatures) - tức là tập hợp các function selector đặc trưung mà chỉ loại contract đó mới có. 

### 4.1. Sàn giao dịch phi tập trung (DEX) & AMM

Các giao thức AMM như Uniswap đã trở thành tiêu chuẩn defacto, và hàng nghìn bản fork của chúng tồn tại trên các chain. Việc nhận diện dựa trên các hàm cốt lõi của Router và Factory. 

* **Uniswap V2 Router:** Là cổng giao tiếp chính của người dùng thực hiện swap. 
  * Hàm đặc trưng 1: `swapExactTokensForTokens(uint, uint, address, address, uint)` -> Selector `0x38ed1739`.
  * Hàm đặc trưng 2: `addLiquidity(...)` -> Selector `0xe8e33700`.
  * Hàm đặc trưng 3: `removeLiquidity(...)`

* **Uniswap V2 Factory:** Chịu trách nhiệm tạo các cặp thanh khoản (Pair)
  * Hàm đặc trưng: `createPair (address,address)` -> `0xc9c65396`.
  * Hàm truy vấn: `getPair(address,address)` -> `0xe6a43905`.

* **Uniswap V3:** Kiến trúc thay đổi với khái niệm thanh khoản tập trung
  * Router V3 sử dụng `exactInput`, `exactOutput` và đặc biệt `multicall(bytes)` (`0xac9650d8`) để thực hiện nhiều lệnh trong một giao dịch. 
  * Universal Router mới nhất kết hợp cả V2 và V3, có hàm `execute(bytes,bytes)` (`0x3593564c`).   

**Chiến lược:** Nếu một contract chứa tổ hợp các selector `createPair` và `getPair`, nó là DEX Factory. Nếu chứa `swapExactTokensForTokens` và `addLiquidity`, nó là DEX Router. 


**Tham khảo:**
* https://docs.uniswap.org/contracts/v2/reference/smart-contracts/router-02
* https://github.com/Uniswap/v2-periphery/blob/master/contracts/test/RouterEventEmitter.sol
* https://docs.uniswap.org/contracts/v2/reference/smart-contracts/factory
* https://medium.com/@qq549631282/learning-smart-contract-by-uniswap-4-factory-and-router-c3fcc103105e
* https://docs.uniswap.org/contracts/universal-router/technical-reference


### 4.2. Oracle Networks (Chainlink)
ChainLink là mạng lưới Oracle phổ biến nhất. Các contract Price Feed của Chainlink tuân thủ nghiêm ngặt `AggregatorV3Interface`.
* Chu kỳ nhận diện:
  * Hàm quan trọng nhất: `latestRoundData()`. Hàm này trả về dữ liệu giá mới nhất cùng timestamp. Selector: `0xfeaf968c`
  * Các hàm metadata: `decimals()` (`0x313ce567`), `description()` (`0x7284e416`), `version()` (`0x54fd4d50`).

* Chiến lược: Bất kỳ contract nào có hàm `latestRoundData` trả về cấu trúc `(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)` đều có thể phân loại an toàn là **Oracle/Chainlink Price Feed.**


### 4.3. Cross-chain Bridges (LayerZero & Wormhole)

Các giao thức cầu nối (Bridge) có độ phức tạp cao, nhưng các Endpoints và Relayers của chúng có các hàm giao tiếp on-chain rất đặc thù. 

* **LayerZero (Omnichain Interoperability Protocol):**
  * Các ứng dụng OApp (Omnichain Applications) phải triển khai giao diện tin nhắn. 
  * Hàm nhận tin: `lzReceive(uint16, bytes, uint64, bytes)`. Sự hiện diện của hàm này (hoặc `_lzReceive`) trong implementation nội bộ) là dấu hiệu của một OApp.
  * Hàm gửi tin: (tại Endpoint): `lzSend` hoặc `send`. Trong V2, cấu trúc `lzSend` bao gồm các tham số tuỳ chọn (`_options`) để cấu hình gas và executor. 
* **Wormhole:**
  * Core Bridge Contract chịu trách nhiệm phát và xác thực tin nhắn (VAA).
  * Hàm gửi: `publishMessage` hoặc `sendMessage`.
  * Hàm nhận: Các contract tích hợp Wormhole thường có hàm kiểm tra `parseAndVerifyVM` để xác thực chữ ký của Guardians. 
  * Đặc điểm nhận dạng: Các contract Bridge thường có các hàm `transferTokens` (cho Token Bridge) hoặc `attestToken`. Selector của `sendMessage` cần được phân tích kỹ từ ABI cụ thể của từng phiên bản Wormhole Relayer. 

**Tham khảo:**
* https://docs.layerzero.network/v2/concepts/protocol/layerzero-endpoint
* https://docs.layerzero.network/v2/developers/evm/getting-started
* https://docs.layerzero.network/v2/developers/evm/configuration/options
* https://docs.layerzero.network/v2/developers/evm/oapp/overview
* https://wormhole-foundation.github.io/js-wormhole-sdk/
* https://wormhole.com/docs/products/messaging/tutorials/cross-chain-contracts/
* https://www.quicknode.com/guides/cross-chain/wormhole/how-to-create-a-cross-chain-messaging-app

## 5. Phân tích vòng đời Contract (Lifecycle Analysis) (Optional)

Trường `Lifecycle` yêu cầu xác định hai thông tin lịch sử: **Creation Block**  (Block mà contract được tạo ra) và **Creator Address** 
(Địa chỉ ví hoặc contract đã tạo ra nó). Ethereum State hiện tại không lưu trữ thông tin này, do đó ta phải sử dụng các kỹ thuật "khảo cổ học" blockchain.

### 5.1. Thuật Toán Binary Search Tìm Creation Block
Việc quét tuần tự (Linear Scan) từ block 0 đến block hiện tại để tìm thời điểm contract xuất hiện là bất khả thi về mặt thời gian. 
Giải pháp tối ưu là sử dụng thuật toán Binary Search (Tìm kiếm nhị phân) dựa trên sự tồn tại của mã bytecode.


**Quy trình thuật toán:**

1. **Khởi tạo:** Thiết lập khoảng tìm kiếm ``. MinBlock là 0, MaxBlock là block hiện tại ("latest").

2. **Vòng lặp:**

* Tính `MidBlock = (MinBlock + MaxBlock) / 2.`

* Gọi `web3.eth.getCode(contractAddress, MidBlock)`.

* **Điều kiện 1:** Nếu kết quả trả về là 0x (rỗng) -> Tại thời điểm MidBlock, contract chưa tồn tại. Vậy Creation Block phải nằm sau MidBlock. Gán MinBlock = MidBlock + 1.

* **Điều kiện 2:** Nếu kết quả trả về có bytecode -> Tại thời điểm MidBlock, contract đã tồn tại. Vậy Creation Block có thể là MidBlock hoặc trước đó. Gán MaxBlock = MidBlock.

3. **Kết thúc:** Khi MinBlock == MaxBlock, đó chính là Creation Block chính xác.

Lưu ý quan trọng về `SELFDESTRUCT`: Nếu một contract bị hủy bằng lệnh `SELFDESTRUCT` và sau đó được tạo lại (dùng `CREATE2`), thuật toán trên có thể tìm ra block của lần tạo gần nhất hoặc bị sai lệch nếu không xử lý kỹ logic kiểm tra code tại biên.

### 5.2. Truy Vết Creator Address và Transaction Trace
Sau khi đã xác định được Creation Block, bước tiếp theo là phân tích dữ liệu trong block đó để tìm người tạo.   

* **Trường hợp 1:** Contract được tạo bởi EOA (Externally Owned Account) thông qua giao dịch trực tiếp.

  * Quét danh sách transactions trong Creation Block.

  * Tìm giao dịch có trường to là null (hoặc 0x0) và trường receipt.contractAddress trùng với địa chỉ contract cần tìm.

  * Người tạo (Creator) chính là tx.from của giao dịch đó.

* **Trường hợp 2:** Contract được tạo bởi một Contract khác (Factory Pattern - Internal Transaction).

  * Đây là trường hợp phổ biến với Uniswap Pairs, Gnosis Safes, hoặc Minimal Proxies. Contract không được tạo bởi một giao dịch to: null mà bởi một lệnh CREATE hoặc CREATE2 bên trong một giao dịch gọi đến Factory.

  * Giải pháp: Cần sử dụng RPC method debug_traceTransaction hoặc trace_block (nếu Node hỗ trợ Archival/Tracing). Phân tích dấu vết (trace) để tìm opcode CREATE/CREATE2 tạo ra địa chỉ mục tiêu. Địa chỉ thực thi opcode này (Factory) là người tạo trực tiếp. Trong mô hình dữ liệu, ta có thể lưu cả Factory Address và tx.origin (EOA kích hoạt).

* **Trường hợp 3:** Phát hiện qua Sự kiện (Event Analysis).

  * Các Factory chuẩn mực thường emit sự kiện khi tạo contract con. Ví dụ: Uniswap V2 Factory emit sự kiện PairCreated(address indexed token0, address indexed token1, address pair, uint).

  * Bằng cách lọc logs trong Creation Block với topic của PairCreated, ta có thể xác định ngay lập tức Factory mà không cần chạy trace phức tạp.   

  
**Tham khảo:**
* https://ethereum.stackexchange.com/questions/65194/get-creator-from-contract-address
* https://gist.github.com/jerryan999/56ffce6571cfd8ef06e96d580b252b32


## 6. Tổng hợp kiến trúc hệ thống tự động hoá
Dựa trên các phân tích chi tiết ở trên, kiến trúc của hệ thống tự động điền thôg tin cho `EthContract` sẽ tuân theo luồng xử lý pipeline sau:
1. **Input:** Nhận địa chỉ Contract Address cần phân tích. 
2. **Phase 1:** Phân tích tĩnh (Static Analysis)
   * Lấy bytecode runtime mới nhất từ node. 
   * Dùng disassembler trích xuất toàn bộ Function Selectors. 
   * So khớp selector với database để gán nhãn sỡ bộ (ERC-20 Functions, Owner Functions)
3. **Phase 2:** Kiểm tra tiêu chuẩn (Standard Compliance)
   * Thực hiện gọi `supportsInterface` cho ERC-721 và ERC-1155
   * Nếu thất bại, áp dụng heuristic (kiểm tra 6 hàm cơ bản) cho ERC-20.
   * Output: Token Standard (nếu có).
4. **Phase 3:** Phát Hiện Proxy (Proxy Detection)
   * Kiểm tra mẫu bytecode EIP-1167 (Minimal Proxy)
   * Đọc Storage Slot EIP-1967 và Gnosis Safe Slot 0. 
   * Nếu phát hiện Proxy, đệ quy trình phân tích cho địa chỉ Implementation tìm được. 
   * Output: Proxy Pattern (Transparent/UUPS/Minimal/Diamond/Safe) và Implementation Address.
5. **Phase 4:** Phân loại danh mục (Categorization)
   * Dựa trên tập hợp Selector và kết quả Proxy, phân loại contract vào nhóm DEX, Oracle, Bridge, hay Multisig Wallet. 
   * Output: Category.
6. **Phase 5:** Truy Vết Lịch Sử (Lifecycle Tracing) (Optional)
   * Chạy Binary Search để tìm Creation Block.
   * Quét Transaction/Trace/Log trong block đó để tìm Creator.
   * Output: Creation Block, Timestamp, Creator Address.

Hệ thống này đảm bảo tính bao quát và độ chính xác cao, xử lý được cả các trường hợp contract tiêu chuẩn lẫn các mô hình phức tạp như Proxy đa tầng hay các giao thức Cross-chain hiện đại. 
Sự kết hợp giữa các phương pháp xác định tất định (deterministic) và quy tắc kinh nghiệm (heuristic) là chìa khóa để giải mã "hộp đen" bytecode trên Ethereum.

