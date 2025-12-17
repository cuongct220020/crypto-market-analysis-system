from typing import Dict, Any
from ingestion.ethereumetl.models.contract import EthBaseContract, EthProxyContract, EthImplementationContract

class EthContractMapper:
    @staticmethod
    def contract_to_dict(contract: EthBaseContract) -> Dict[str, Any]:
        """
        Maps polymorphism models to a single flat Avro dictionary.
        Respects 'impl_category' naming convention from Python models.
        """
        data = contract.model_dump(mode='json', by_alias=True)

        if isinstance(contract, EthProxyContract):
            # For Proxy, we try to enrich it with implementation details if available
            if contract.implementation:
                impl = contract.implementation
                
                # Flatten metadata
                if not data.get("name"): data["name"] = impl.name
                if not data.get("symbol"): data["symbol"] = impl.symbol
                if not data.get("decimals"): data["decimals"] = impl.decimals
                if not data.get("total_supply"): data["total_supply"] = impl.total_supply
                
                # Flatten standards
                data["is_erc20"] = impl.is_erc20
                data["is_erc721"] = impl.is_erc721
                data["is_erc1155"] = impl.is_erc1155
                data["supports_erc165"] = impl.supports_erc165
                
                # Flatten Classification (Exact field match)
                if impl.impl_category and impl.impl_category != "UNKNOWN":
                    data["impl_category"] = impl.impl_category.value
                
                data["impl_detected_by"] = impl.impl_detected_by
                data["impl_classify_confidence"] = impl.impl_classify_confidence

                # Remove nested object
                if "implementation" in data:
                    del data["implementation"]
        
        elif isinstance(contract, EthImplementationContract):
            # Ensure enum extraction just in case model_dump mode='json' didn't catch it deeply
            # (Though mode='json' usually handles Enums correctly)
            if contract.impl_category:
                 data["impl_category"] = contract.impl_category.value

        return data