# constants/event_transfer_signature.py

# ERC-20 / ERC-721 Transfer(address indexed from, address indexed to, uint256 value/tokenId)
# This is the SHA3 hash of the event signature "Transfer(address,address,uint256)"
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ERC-1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
# This is the SHA3 hash of the event signature "TransferSingle(address,address,address,uint256,uint256)"
TRANSFER_SINGLE_EVENT_SIGNATURE = "0xc3d58168c5aff7974da1a66a788d1049519506fa80b4184e5d6d9da9620699c4"

# ERC-1155 TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
# This is the SHA3 hash of the event signature "TransferBatch(address,address,address,uint256[],uint256[])"
TRANSFER_BATCH_EVENT_SIGNATURE = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
