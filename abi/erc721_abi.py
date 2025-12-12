ERC721_ABI = [
    # --- READ FUNCTIONS (Dành cho EthContract) ---
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    # Hàm quan trọng nhất để xác định "is_erc721" = True
    {
        "constant": True,
        "inputs": [{"name": "interfaceId", "type": "bytes4"}],
        "name": "supportsInterface",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    },
    # --- EVENTS (Dành cho EthTokenTransfer) ---
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": True, "name": "tokenId", "type": "uint256"}
            # Lưu ý: ERC721 tokenId ĐƯỢC indexed (Khác với ERC20)
        ],
        "name": "Transfer",
        "type": "event"
    }
]