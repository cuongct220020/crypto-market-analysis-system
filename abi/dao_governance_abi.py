# --- GOVERNOR (DAO Governance - Generic Bravo/OpenZeppelin) ---
GOVERNOR_ABI = [
    {
        "inputs": [
            {"internalType": "address[]", "name": "targets", "type": "address[]"},
            {"internalType": "uint256[]", "name": "values", "type": "uint256[]"},
            {"internalType": "bytes[]", "name": "calldatas", "type": "bytes[]"},
            {"internalType": "string", "name": "description", "type": "string"}
        ],
        "name": "propose",
        "outputs": [{"internalType": "uint256", "name": "proposalId", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "uint256", "name": "proposalId", "type": "uint256"},
            {"internalType": "uint8", "name": "support", "type": "uint8"}
        ],
        "name": "castVote",
        "outputs": [{"internalType": "uint256", "name": "balance", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "uint256", "name": "proposalId", "type": "uint256"}],
        "name": "state",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
]