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

4. **Configure OLI and submit:**

   ```bash
   export OLI_PRIVATE_KEY="your_private_key_here"
   python -c "
   from local_data_processor import LocalSourcifyProcessor
   from oli_submitter import OLISubmitter
   import os

   processor = LocalSourcifyProcessor()
   submitter = OLISubmitter(os.getenv('OLI_PRIVATE_KEY'), is_production=False)

   # Process all contracts in batches
   for batch in processor.process_all_contracts(batch_size=1000):
       successful, total = submitter.submit_batch(batch, submit_onchain=False, delay=1.0)
       print(f'Batch: {successful}/{total} successful')
   "
   ```

## Components

- **`download_parquet_files.py`** - Downloads Sourcify Parquet exports locally
- **`local_data_processor.py`** - Processes local files with DuckDB for efficient joins
- **`test_local_processing.py`** - Comprehensive test suite
- **`oli_submitter.py`** - Handles OLI validation and submission

## Testing

**Run comprehensive tests:**

```bash
python test_local_processing.py
```

Tests validate:

- Data availability and integrity
- DuckDB join performance
- OLI tag generation quality
- Batch processing functionality
- Integration with OLI submitter

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
