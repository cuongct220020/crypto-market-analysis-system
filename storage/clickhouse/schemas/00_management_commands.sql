-- ==========================================
-- CLICKHOUSE MANAGEMENT COMMANDS
-- Use these commands with EXTREME CAUTION.
-- DROPPING objects will result in in PERMANENT DATA LOSS.
-- ==========================================

-- !!! RUN THESE COMMANDS INDIVIDUALLY OR IN SPECIFIC BLOCKS !!!
-- Do NOT run this entire file blindly, especially DROP commands.

-- --- DATABASE MANAGEMENT ---
-- Drop the entire 'crypto' database (ALL tables, views, etc. will be deleted)
-- DROP DATABASE IF EXISTS crypto;


-- --- TABLE DROPPING COMMANDS ---
-- Drop Materialized Views first, then Kafka Queue Tables, then Target Tables.
-- This order is CRUCIAL to avoid issues with dependencies.

-- BLOCK: Drop all Crypto Ethereum tables
-- Please uncomment and run each section if you intend to drop.

-- DROP VIEWS FIRST
-- DROP VIEW IF EXISTS crypto.blocks_mv;
-- DROP VIEW IF EXISTS crypto.transactions_mv;
-- DROP VIEW IF EXISTS crypto.logs_mv;
-- DROP VIEW IF EXISTS crypto.token_transfers_mv;
-- DROP VIEW IF EXISTS crypto.contracts_mv;

-- DROP KAFKA QUEUE TABLES NEXT
-- DROP TABLE IF EXISTS crypto.kafka_blocks_queue;
-- DROP TABLE IF EXISTS crypto.kafka_transactions_queue;
-- DROP TABLE IF EXISTS crypto.kafka_receipts_queue; -- Queue for logs
-- DROP TABLE IF EXISTS crypto.kafka_token_transfers_queue;
-- DROP TABLE IF EXISTS crypto.kafka_contracts_queue;

-- DROP TARGET TABLES LAST
-- DROP TABLE IF EXISTS crypto.blocks;
-- DROP TABLE IF EXISTS crypto.transactions;
-- DROP TABLE IF EXISTS crypto.logs;
-- DROP TABLE IF EXISTS crypto.token_transfers;
-- DROP TABLE IF EXISTS crypto.contracts;


-- --- GENERAL TABLE MANAGEMENT COMMANDS ---

-- Describe table structure
-- DESCRIBE TABLE crypto.blocks;
-- DESCRIBE TABLE crypto.transactions;
-- DESCRIBE TABLE crypto.logs;
-- DESCRIBE TABLE crypto.token_transfers;
-- DESCRIBE TABLE crypto.contracts;

-- SHOW CREATE TABLE to see full definition
-- SHOW CREATE TABLE crypto.blocks;

-- Alter table (example: add a new column - remember to update MVs and ingestion if needed)
-- ALTER TABLE crypto.blocks ADD COLUMN new_metadata String DEFAULT '';

-- Alter table (example: drop a column)
-- ALTER TABLE crypto.blocks DROP COLUMN new_metadata;

-- Optimize table (merges parts, useful after many small insertions)
-- OPTIMIZE TABLE crypto.blocks FINAL;

-- Check table integrity
-- CHECK TABLE crypto.blocks;
