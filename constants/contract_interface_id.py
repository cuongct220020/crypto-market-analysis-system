# from web3 import Web3
#
# # Các Interface ID chuẩn
# ERC165_ID = "0x01ffc9a7"
# ERC721_ID = "0x80ac58cd"
# ERC1155_ID = "0xd9b67a26"
#
#
# def enrich_standard_info(w3: Web3, contract_address: str, model: EthContract) -> EthContract:
#     contract = w3.eth.contract(address=contract_address, abi=[{
#         "constant": True,
#         "inputs": [{"name": "interfaceId", "type": "bytes4"}],
#         "name": "supportsInterface",
#         "outputs": [{"name": "", "type": "bool"}],
#         "type": "function"
#     }])
#
#     try:
#         # 1. Kiểm tra ERC-165 trước
#         # Nếu call này fail (revert), nghĩa là contract không có hàm supportsInterface
#         supports_165 = contract.functions.supportsInterface(ERC165_ID).call()
#
#         if supports_165:
#             model.supports_erc165 = True
#             model.detected_by.append("erc165")
#
#             # 2. Nếu đã support 165, hỏi tiếp xem có phải 721 không
#             is_721 = contract.functions.supportsInterface(ERC721_ID).call()
#             if is_721:
#                 model.is_erc721 = True
#                 model.category = ContractCategory.NFT
#                 model.detected_by.append("erc165:erc721")
#
#             # 3. Hỏi xem có phải 1155 không
#             is_1155 = contract.functions.supportsInterface(ERC1155_ID).call()
#             if is_1155:
#                 model.is_erc1155 = True
#                 model.category = ContractCategory.MULTI_TOKEN
#                 model.detected_by.append("erc165:erc1155")
#
#     except Exception as e:
#         # Contract cũ hoặc không hỗ trợ chuẩn
#         model.supports_erc165 = False
#         # Lúc này bạn phải fallback về cách check function_sighashes thủ công
#         # Ví dụ: check xem có hàm "balanceOfBatch" (đặc trưng của 1155) không
#         pass
#
#     return model
#
#
# # Định nghĩa các Signature đặc trưng (Hardcode để chạy nhanh)
# SIGS = {
#     "ERC1155_BATCH_TRANSFER": "0x2eb2c2d6",
#     "UNI_FACTORY_GET_PAIR": "0xe6a43905",
#     "UNI_ROUTER_SWAP": "0x38ed1739",
#     "ERC4626_ASSET": "0x38d52e0f",
#     "CHAINLINK_LATEST_ROUND": "0xfeaf968c"
# }