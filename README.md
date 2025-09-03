# Sourcify OLI Bridge

> ⚠️ **This README and the code is AI-generated**

Push verified smart contract labels from Sourcify to the Open Labels Initiative (OLI). Processes 8.8+ million verified contracts efficiently using locally downloaded Parquet files and DuckDB.

## OLI Tags Generated

- `source_code_verified`: "sourcify"
- `is_contract`: true
- `code_language`: solidity, vyper, etc.
- `code_compiler`: compiler version used
- `deployment_block`: deployment block number
- `deployment_tx`: deployment transaction hash
- `deployer_address`: deployer address
- `contract_name`: contract name from compilation metadata

## Quick Start

1. **Setup environment:**

   ```bash
   git clone <repository>
   cd oli-sourcify-labels
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Download Sourcify data (~10-15GB):**

   ```bash
   python download_parquet_files.py
   ```

3. **Test the pipeline:**

   ```bash
   python test_local_processing.py
   ```

4. **Configure OLI and process all contracts:**
   ```bash
   export OLI_PRIVATE_KEY="your_private_key_here"
   python main.py
   ```

## Components

- **`main.py`** - Main entry point for processing all contracts
- **`download_parquet_files.py`** - Downloads Sourcify Parquet exports locally
- **`local_data_processor.py`** - Processes local files with DuckDB for efficient joins
- **`test_local_processing.py`** - Comprehensive test suite
- **`oli_submitter.py`** - Handles OLI validation and submission

## Testing

**Run comprehensive tests:**
```bash
python test_local_processing.py
```
Tests validate data integrity, joins, OLI tag generation, and full pipeline functionality.

**Quick processor test:**
```bash
python local_data_processor.py  
```
Shows data statistics, join results, and OLI tag preview.

## Configuration

**Required:**
- `OLI_PRIVATE_KEY` - Your private key (wallet must have ETH on Base)

**Optional:**
- `USE_PRODUCTION="true"` - Use Base Mainnet (default: Base Sepolia testnet)
- `DEFAULT_BATCH_SIZE="5000"` - Contracts per batch (default: 1000)  
- `SUBMISSION_DELAY="0.5"` - Delay between submissions in seconds (default: 1.0)
- `SUBMIT_ONCHAIN="true"` - Submit onchain (costs gas, default: false)
- `MAX_PARALLEL_WORKERS="20"` - Parallel workers for offchain submissions (default: 10)
- `DATA_DIR="./my_data"` - Data directory path (default: ./sourcify_data)

## Production Usage

**For production (Base Mainnet) with onchain attestations:**
```bash
export OLI_PRIVATE_KEY="your_private_key"
export USE_PRODUCTION="true"
export SUBMIT_ONCHAIN="true"
export DEFAULT_BATCH_SIZE="2000"
python main.py
```

**For testnet (Base Sepolia) with free offchain submissions:**
```bash
export OLI_PRIVATE_KEY="your_private_key"
python main.py
```

## How It Works

1. **Downloads** all Sourcify Parquet files (~10-15GB) to `./sourcify_data/`
2. **Joins** 3 tables locally using DuckDB hash tables:
   - `verified_contracts` ⟵⟶ `contract_deployments`
   - `verified_contracts` ⟵⟶ `compiled_contracts`
3. **Processes** contracts in batches with OLI tag generation
4. **Submits** to OLI platform (offchain free, onchain requires gas)

**Performance:** ~1GB RAM, processes millions of contracts efficiently without API rate limits.

## Data Schema

- **verified_contracts**: `id`, `deployment_id`, `compilation_id`, `created_at`
- **contract_deployments**: `id`, `chain_id`, `address`, `transaction_hash`, `block_number`, `deployer`
- **compiled_contracts**: `id`, `language`, `compiler`, `version`
