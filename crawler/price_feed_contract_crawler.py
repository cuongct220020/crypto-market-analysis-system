import os
import re
import requests
from typing import Dict, List

BASE_URL = "https://data.chain.link/feeds/ethereum/mainnet/"
TIMEOUT = 15

feeds_slugs: List[str] = [
    # ===== Major =====
    "eth-usd", "btc-usd", "link-usd", "eth-btc", "btc-eth",

    # ===== Stablecoins =====
    "usdt-usd", "usdc-usd", "dai-usd", "frax-usd", "tusd-usd",
    "usdp-usd", "lusd-usd",

    # ===== ETH pairs =====
    "usdc-eth", "usdt-eth", "dai-eth", "link-eth", "uni-eth",
    "aave-eth", "snx-eth", "mkr-eth", "crv-eth", "comp-eth",
    "yfi-eth", "sushi-eth", "1inch-eth",

    # ===== USD pairs =====
    "aave-usd", "uni-usd", "snx-usd", "mkr-usd", "crv-usd",
    "comp-usd", "yfi-usd", "sushi-usd", "1inch-usd",
    "ens-usd", "ldo-usd", "rpl-usd",

    # ===== L2 / Infra =====
    "arb-usd", "avax-usd"

    # ===== Other majors =====
    "bnb-usd", "sol-usd",

    # ===== Commodities / FX =====
    "xau-usd", "xag-usd", "eur-usd", "gbp-usd", "jpy-usd",
]


ADDRESS_REGEX = re.compile(r"0x[a-fA-F0-9]{40}")


def is_valid_feed(slug: str) -> bool:
    try:
        r = requests.head(
            f"{BASE_URL}{slug}",
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        return r.status_code == 200
    except Exception:
        return False


def fetch_contract_address(slug: str) -> str | None:
    try:
        r = requests.get(f"{BASE_URL}{slug}", timeout=TIMEOUT)
        if r.status_code != 200:
            return None

        match = ADDRESS_REGEX.search(r.text)
        return match.group(0) if match else None

    except Exception:
        return None


def collect_price_feeds(slugs: List[str]) -> Dict[str, str]:
    results: Dict[str, str] = {}

    for slug in slugs:
        print(f"🔍 Fetching {slug} ...")

        if not is_valid_feed(slug):
            print(f"  ❌ Invalid feed page")
            continue

        address = fetch_contract_address(slug)
        if not address:
            print(f"  ❌ Address not found")
            continue

        results[slug] = address
        print(f"  ✅ {address}")

    return results



def load_existing_price_feeds(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}

    namespace = {}
    with open(path, "r", encoding="utf-8") as f:
        exec(f.read(), namespace)

    return namespace.get("PRICE_FEED_CONTRACT_ADDRESS", {})


def export_python_dict(data: Dict[str, str], path: str):
    existing = load_existing_price_feeds(path)

    # merge (data mới override data cũ nếu trùng key)
    merged = {**existing, **data}

    with open(path, "w", encoding="utf-8") as f:
        f.write("PRICE_FEED_CONTRACT_ADDRESS = {\n")
        for k, v in sorted(merged.items()):
            f.write(f'    "{k}": "{v}",\n')
        f.write("}\n")


def main():
    price_feeds = collect_price_feeds(feeds_slugs)
    output_file = "../constants/price_feed_contract_address.py"

    print("\nCollected feeds:", len(price_feeds))

    export_python_dict(price_feeds, output_file)

    print(f"Output files: {output_file}")

if __name__ == "__main__":
    main()