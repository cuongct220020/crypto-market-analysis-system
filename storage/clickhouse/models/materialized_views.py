MATERIALIZED_VIEWS_SQL = [
    # Blocks MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS blocks_mv TO blocks AS
    SELECT
        ifNull(type, 'block') AS type,
        ifNull(chain_id, 0) AS chain_id,
        ifNull(number, 0) AS number,
        ifNull(hash, '') AS hash,
        ifNull(parent_hash, '') AS parent_hash,
        ifNull(nonce, '') AS nonce,
        ifNull(sha3_uncles, '') AS sha3_uncles,
        ifNull(logs_bloom, '') AS logs_bloom,
        ifNull(transactions_root, '') AS transactions_root,
        ifNull(state_root, '') AS state_root,
        ifNull(receipts_root, '') AS receipts_root,
        ifNull(miner, '') AS miner,
        CAST(ifNull(difficulty, '0') AS UInt256) AS difficulty,
        CAST(ifNull(total_difficulty, '0') AS UInt256) AS total_difficulty,
        ifNull(size, 0) AS size,
        ifNull(extra_data, '') AS extra_data,
        ifNull(gas_limit, 0) AS gas_limit,
        ifNull(gas_used, 0) AS gas_used,
        ifNull(base_fee_per_gas, 0) AS base_fee_per_gas,
        ifNull(blob_gas_used, 0) AS blob_gas_used,
        ifNull(excess_blob_gas, 0) AS excess_blob_gas,
        CAST(ifNull(transaction_count, 0) AS UInt32) AS transaction_count,
        ifNull(timestamp, 0) AS timestamp,
        ifNull(withdrawals_root, '') AS withdrawals_root,
        ifNull(parent_beacon_block_root, '') AS parent_beacon_block_root
    FROM kafka_blocks_queue;
    """,
    # Withdrawals MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS withdrawals_mv TO withdrawals AS
    SELECT
        CAST(number AS UInt64) AS block_number,
        CAST(timestamp AS UInt64) AS block_timestamp,
        hash AS block_hash,
        CAST(ifNull(withdrawal_index, 0) AS UInt64) AS index,
        CAST(ifNull(withdrawal_validator_index, 0) AS UInt64) AS validator_index,
        ifNull(withdrawal_address, '') AS address,
        CAST(ifNull(withdrawal_amount, '0') AS UInt256) AS amount
    FROM crypto.kafka_blocks_queue
    ARRAY JOIN
        `withdrawals.index` AS withdrawal_index,
        `withdrawals.validator_index` AS withdrawal_validator_index,
        `withdrawals.address` AS withdrawal_address,
        `withdrawals.amount` AS withdrawal_amount
    WHERE length(`withdrawals.index`) > 0;
    """,
    # Transactions MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_mv TO transactions AS
    SELECT
        ifNull(type, 'transaction') AS type,
        ifNull(hash, '') AS hash,
        ifNull(nonce, 0) AS nonce,
        ifNull(chain_id, 0) AS chain_id,
        ifNull(block_hash, '') AS block_hash,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_timestamp, 0) AS block_timestamp,
        CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
        ifNull(from_address, '') AS from_address,
        ifNull(to_address, '') AS to_address,
        CAST(ifNull(value, '0') AS UInt256) AS value,
        ifNull(gas, 0) AS gas,
        ifNull(gas_price, 0) AS gas_price,
        ifNull(input, '') AS input,
        ifNull(max_fee_per_gas, 0) AS max_fee_per_gas,
        ifNull(max_priority_fee_per_gas, 0) AS max_priority_fee_per_gas,
        CAST(ifNull(transaction_type, 0) AS UInt8) AS transaction_type,
        ifNull(max_fee_per_blob_gas, 0) AS max_fee_per_blob_gas,
        blob_versioned_hashes,
        ifNull(receipt_cumulative_gas_used, 0) AS receipt_cumulative_gas_used,
        ifNull(receipt_effective_gas_price, 0) AS receipt_effective_gas_price,
        ifNull(receipt_gas_used, 0) AS receipt_gas_used,
        ifNull(receipt_blob_gas_price, 0) AS receipt_blob_gas_price,
        ifNull(receipt_blob_gas_used, 0) AS receipt_blob_gas_used,
        ifNull(receipt_contract_address, '') AS receipt_contract_address,
        CAST(ifNull(receipt_status, 0) AS UInt8) AS receipt_status,
        ifNull(receipt_root, '') AS receipt_root
    FROM kafka_transactions_queue;
    """,
    # Logs MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS logs_mv TO logs AS
    SELECT
        ifNull(L_block_number, 0) AS block_number,
        ifNull(L_block_timestamp, 0) AS block_timestamp,
        ifNull(L_block_hash, '') AS block_hash,
        ifNull(L_transaction_hash, '') AS transaction_hash,
        CAST(ifNull(L_transaction_index, 0) AS UInt32) AS transaction_index,
        CAST(ifNull(L_log_index, 0) AS UInt32) AS log_index,
        ifNull(L_address, '') AS address,
        if(notEmpty(L_topics), L_topics[1], '') AS topic0,
        L_topics AS topics,
        ifNull(L_data, '') AS data
    FROM kafka_receipts_queue
    ARRAY JOIN
        `logs.block_number` AS L_block_number,
        `logs.block_timestamp` AS L_block_timestamp,
        `logs.block_hash` AS L_block_hash,
        `logs.transaction_hash` AS L_transaction_hash,
        `logs.transaction_index` AS L_transaction_index,
        `logs.log_index` AS L_log_index,
        `logs.address` AS L_address,
        `logs.topics` AS L_topics,
        `logs.data` AS L_data;
    """,
    # Receipts MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS receipts_mv TO receipts AS
    SELECT
        ifNull(type, 'receipt') AS type,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_hash, '') AS block_hash,
        ifNull(transaction_hash, '') AS transaction_hash,
        CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
        ifNull(from_address, '') AS from_address,
        ifNull(to_address, '') AS to_address,
        ifNull(contract_address, '') AS contract_address,
        ifNull(cumulative_gas_used, 0) AS cumulative_gas_used,
        ifNull(gas_used, 0) AS gas_used,
        ifNull(effective_gas_price, 0) AS effective_gas_price,
        ifNull(blob_gas_used, 0) AS blob_gas_used,
        ifNull(blob_gas_price, 0) AS blob_gas_price,
        CAST(ifNull(status, 0) AS UInt8) AS status,
        ifNull(root, '') AS root,
        ifNull(logs_bloom, '') AS logs_bloom
    FROM kafka_receipts_queue;
    """,
    # Token Transfers MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_mv TO token_transfers AS
    SELECT
        ifNull(type, 'token_transfer') AS type,
        ifNull(token_standard, 'unknown') AS token_standard,
        ifNull(transfer_type, 'unknown') AS transfer_type,
        ifNull(contract_address, '') AS contract_address,
        ifNull(operator_address, '') AS operator_address,
        ifNull(from_address, '') AS from_address,
        ifNull(to_address, '') AS to_address,
        CAST(ifNull(token_id_raw, '0') AS UInt256) AS token_id,
        CAST(ifNull(value_raw, '0') AS UInt256) AS value,
        ifNull(erc1155_mode, '') AS erc1155_mode,
        CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
        ifNull(transaction_hash, '') AS transaction_hash,
        CAST(ifNull(log_index, 0) AS UInt32) AS log_index,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_hash, '') AS block_hash,
        ifNull(block_timestamp, 0) AS block_timestamp,
        ifNull(chain_id, 0) AS chain_id,
        item_id,
        item_timestamp
    FROM kafka_token_transfers_queue
    ARRAY JOIN 
        `amounts.token_id` AS token_id_raw, 
        `amounts.value` AS value_raw
    WHERE length(`amounts.token_id`) > 0;
    """,
    # Contracts MV
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS contracts_mv TO contracts AS
    SELECT
        ifNull(type, 'contract') AS type,
        ifNull(address, '') AS address,
        ifNull(chain_id, 0) AS chain_id,
        ifNull(name, '') AS name,
        ifNull(symbol, '') AS symbol,
        CAST(ifNull(decimals, 0) AS UInt8) AS decimals,
        CAST(ifNull(total_supply, '0') AS UInt256) AS total_supply,
        ifNull(bytecode, '') AS bytecode,
        ifNull(bytecode_hash, '') AS bytecode_hash,
        function_sighashes,
        CAST(ifNull(is_proxy, 0) AS UInt8) AS is_proxy,
        ifNull(proxy_type, '') AS proxy_type,
        ifNull(implementation_address, '') AS implementation_address,
        CAST(ifNull(is_erc20, 0) AS UInt8) AS is_erc20,
        CAST(ifNull(is_erc721, 0) AS UInt8) AS is_erc721,
        CAST(ifNull(is_erc1155, 0) AS UInt8) AS is_erc1155,
        CAST(ifNull(supports_erc165, 0) AS UInt8) AS supports_erc165,
        ifNull(impl_category, '') AS impl_category,
        impl_detected_by,
        ifNull(impl_classify_confidence, 0.0) AS impl_classify_confidence,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_timestamp, 0) AS block_timestamp,
        ifNull(block_hash, '') AS block_hash
    FROM kafka_contracts_queue;
    """
]

KAFKA_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS crypto.kafka_blocks_queue (
        type Nullable(String),
        chain_id Nullable(UInt64),
        number Nullable(UInt64),
        hash Nullable(String),
        mix_hash Nullable(String),
        parent_hash Nullable(String),
        nonce Nullable(String),
        sha3_uncles Nullable(String),
        logs_bloom Nullable(String),
        transactions_root Nullable(String),
        state_root Nullable(String),
        receipts_root Nullable(String),
        miner Nullable(String),

        difficulty Nullable(String),
        total_difficulty Nullable(String),

        size Nullable(UInt64),
        extra_data Nullable(String),
        gas_limit Nullable(UInt64),
        gas_used Nullable(UInt64),
        timestamp Nullable(UInt64),
        transaction_count Nullable(UInt64),
        base_fee_per_gas Nullable(UInt64),
        withdrawals_root Nullable(String),

        `withdrawals.index` Array(Nullable(UInt64)),
        `withdrawals.validator_index` Array(Nullable(UInt64)),
        `withdrawals.address` Array(Nullable(String)),
        `withdrawals.amount` Array(Nullable(String)),

        blob_gas_used Nullable(UInt64),
        excess_blob_gas Nullable(UInt64),
        parent_beacon_block_root Nullable(String)
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.blocks.v0', 'clickhouse_blocks_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
    """,
    """
    CREATE TABLE IF NOT EXISTS crypto.kafka_transactions_queue (
        type Nullable(String),
        hash Nullable(String),
        nonce Nullable(UInt64),
        chain_id Nullable(UInt64),
        block_hash Nullable(String),
        block_number Nullable(UInt64),
        transaction_index Nullable(UInt64),
        from_address Nullable(String),
        to_address Nullable(String),
        value Nullable(String), -- String in Avro
        gas Nullable(UInt64),
        gas_price Nullable(UInt64),
        input Nullable(String),
        max_fee_per_gas Nullable(UInt64),
        max_priority_fee_per_gas Nullable(UInt64),
        transaction_type Nullable(UInt64),
        block_timestamp Nullable(UInt64),
        max_fee_per_blob_gas Nullable(UInt64),
        blob_versioned_hashes Array(String),

        receipt_cumulative_gas_used Nullable(UInt64),
        receipt_effective_gas_price Nullable(UInt64),
        receipt_gas_used Nullable(UInt64),
        receipt_blob_gas_price Nullable(UInt64),
        receipt_blob_gas_used Nullable(UInt64),
        receipt_contract_address Nullable(String),
        receipt_status Nullable(UInt64),
        receipt_root Nullable(String)
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.transactions.v0', 'clickhouse_transactions_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
    """,
    """
    CREATE TABLE IF NOT EXISTS crypto.kafka_receipts_queue (
        type Nullable(String),
        block_hash Nullable(String),
        block_number Nullable(UInt64),
        contract_address Nullable(String),
        cumulative_gas_used Nullable(UInt64),
        effective_gas_price Nullable(UInt64),
        from_address Nullable(String),
        gas_used Nullable(UInt64),
        blob_gas_used Nullable(UInt64),
        blob_gas_price Nullable(UInt64),
        
        `logs.type` Array(Nullable(String)),
        `logs.log_index` Array(Nullable(UInt64)),
        `logs.transaction_hash` Array(Nullable(String)),
        `logs.transaction_index` Array(Nullable(UInt64)),
        `logs.block_hash` Array(Nullable(String)),
        `logs.block_number` Array(Nullable(UInt64)),
        `logs.block_timestamp` Array(Nullable(UInt64)),
        `logs.address` Array(Nullable(String)),
        `logs.data` Array(Nullable(String)),
        `logs.topics` Array(Array(String)),
        
        logs_bloom Nullable(String),
        root Nullable(String),
        status Nullable(UInt64),
        to_address Nullable(String),
        transaction_hash Nullable(String),
        transaction_index Nullable(UInt64)
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.receipts.v0', 'clickhouse_receipts_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
    """,
    """
    CREATE TABLE IF NOT EXISTS crypto.kafka_token_transfers_queue (
        type Nullable(String),
        token_standard Nullable(String),
        transfer_type Nullable(String),
        contract_address Nullable(String),
        operator_address Nullable(String),
        from_address Nullable(String),
        to_address Nullable(String),
        
        `amounts.token_id` Array(Nullable(String)),
        `amounts.value` Array(Nullable(String)),
        
        erc1155_mode Nullable(String),
        transaction_index Nullable(UInt64),
        transaction_hash Nullable(String),
        log_index Nullable(UInt64),
        block_number Nullable(UInt64),
        block_hash Nullable(String),
        block_timestamp Nullable(UInt64),
        chain_id Nullable(UInt64),
        item_id String,
        item_timestamp String
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.token_transfers.v0', 'clickhouse_token_transfers_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
    """,
    """
    CREATE TABLE IF NOT EXISTS crypto.kafka_contracts_queue (
        type Nullable(String),
        address Nullable(String),
        chain_id Nullable(UInt64),
        
        name Nullable(String),
        symbol Nullable(String),
        decimals Nullable(UInt64), -- Avro Long -> UInt64 -> UInt8 in MV
        total_supply Nullable(String),
        
        bytecode Nullable(String),
        bytecode_hash Nullable(String),
        function_sighashes Array(String),
        
        is_proxy Nullable(UInt8), -- Boolean in Avro usually maps to int/long or boolean
        proxy_type Nullable(String),
        implementation_address Nullable(String),
        
        is_erc20 Nullable(UInt8),
        is_erc721 Nullable(UInt8),
        is_erc1155 Nullable(UInt8),
        supports_erc165 Nullable(UInt8),
        
        impl_category Nullable(String),
        impl_detected_by Array(String),
        impl_classify_confidence Nullable(Float32),
        
        block_number Nullable(UInt64),
        block_timestamp Nullable(UInt64),
        block_hash Nullable(String)
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.contracts.v0', 'clickhouse_contracts_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;
    """
]
