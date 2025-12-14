from web3 import Web3

from abi.erc20_abi import ERC20_ABI
from abi.erc721_abi import ERC721_ABI
from abi.erc1155_abi import ERC1155_ABI
from abi.erc4626_abi import ERC4626_ABI
from abi.uniswap_v2_router_abi import UNISWAP_V2_ROUTER_ABI
from abi.uniswap_v2_factory_abi import UNISWAP_V2_FACTORY_ABI
from abi.uniswap_v3_router_abi import UNISWAP_V3_ROUTER_ABI
from abi.uniswap_v3_factory_abi import UNISWAP_V3_FACTORY_ABI
from abi.chainlink_abi import CHAINLINK_AGGREGATOR_ABI
from abi.dao_governance_abi import GOVERNOR_ABI


# Gom hết vào một dict để dễ xử lý
ALL_ABIS = {
    "ERC20": ERC20_ABI,
    "ERC721": ERC721_ABI,
    "ERC1155": ERC1155_ABI,
    "ERC4626": ERC4626_ABI,
    "UNI_V2_ROUTER": UNISWAP_V2_ROUTER_ABI,
    "UNI_V2_FACTORY": UNISWAP_V2_FACTORY_ABI,
    "UNI_V3_ROUTER": UNISWAP_V3_ROUTER_ABI,
    "UNI_V3_FACTORY": UNISWAP_V3_FACTORY_ABI,
    "CHAINLINK": CHAINLINK_AGGREGATOR_ABI,
    "GOVERNOR": GOVERNOR_ABI
}

w3 = Web3()

print("--- GENERATING SIGHASHES ---")
for name, abi in ALL_ABIS.items():
    print(f"\nCategory: {name}")
    contract = w3.eth.contract(abi=abi)
    for func in contract.all_functions():
        # Lấy sighash 4 bytes đầu (hex)
        sig = func.selector
        print(f'    "{func.fn_name}": "{sig}",')