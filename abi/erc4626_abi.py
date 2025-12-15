# --- ERC-4626 (Tokenized Vault Standard) ---
ERC4626_ABI = [
    {
        "inputs": [],
        "name": "asset",
        "outputs": [{"internalType": "address", "name": "assetTokenAddress", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "totalAssets",
        "outputs": [{"internalType": "uint256", "name": "totalManagedAssets", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint256", "name": "assets", "type": "uint256"}],
        "name": "convertToShares",
        "outputs": [{"internalType": "uint256", "name": "shares", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "uint256", "name": "assets", "type": "uint256"},
            {"internalType": "address", "name": "receiver", "type": "address"}
        ],
        "name": "deposit",
        "outputs": [{"internalType": "uint256", "name": "shares", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]