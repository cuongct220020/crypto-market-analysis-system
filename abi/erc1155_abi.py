# --- ERC-1155 (Multi-Token) ---
ERC1155_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_id", "type": "uint256"}
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owners", "type": "address[]"},
            {"name": "_ids", "type": "uint256[]"}
        ],
        "name": "balanceOfBatch",
        "outputs": [{"name": "", "type": "uint256[]"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_from", "type": "address"},
            {"name": "_to", "type": "address"},
            {"name": "_ids", "type": "uint256[]"},
            {"name": "_values", "type": "uint256[]"},
            {"name": "_data", "type": "bytes"}
        ],
        "name": "safeBatchTransferFrom",
        "outputs": [],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "_id", "type": "uint256"}],
        "name": "uri",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "interfaceId", "type": "bytes4"}],
        "name": "supportsInterface",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    }
]