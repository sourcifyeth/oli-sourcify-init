# Sourcify OLI Bridge

Push verified smart contract labels from Sourcify to the Open Labels Initiative (OLI) using their Python SDK. Supports processing 8.8+ million verified contracts efficiently using DuckDB and Parquet exports.

## Overview

This tool processes verified smart contracts from Sourcify and submits them to OLI with the following tags:
- `source_code_verified`: "sourcify"
- `code_language`: Detected from compiler (solidity, vyper, etc.)
- `code_compiler`: Compiler version used
- `deployment_block`: Block number where contract was deployed
- `deployment_tx`: Transaction hash that deployed the contract
- `is_contract`: Always true for verified contracts
- `deployer_address`: Address that deployed the contract

## Architecture

**Two-module design for scalability:**

1. **Data Processing** (`sourcify_data_processor.py`): Uses DuckDB to efficiently process Sourcify's Parquet exports
2. **OLI Submission** (`oli_submitter.py`): Handles validation and submission to OLI platform

## Quick Setup

### Option 1: Automated Setup
```bash
# Clone and setup everything
git clone <repository>
cd oli-sourcify-labels
./setup.sh
```

### Option 2: Manual Setup

1. **Clone and create virtual environment:**
   ```bash
   git clone <repository>
   cd oli-sourcify-labels
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your OLI private key
   ```

## Testing the Data Processing

Test the DuckDB data processing (no OLI credentials needed):

```bash
# Activate virtual environment
source venv/bin/activate

# Test data processing pipeline
python test_data_processing.py
```

This will:
- Connect to Sourcify's public parquet exports
- Process sample verified contracts
- Show data quality statistics
- Preview OLI tag structure

## Data Sources

The tool supports multiple data sources for different use cases:

### 1. Parquet Files (Recommended for Bulk Processing)
- **Source**: Sourcify's public Parquet exports via Cloudflare R2
- **URL**: https://export.sourcify.dev/
- **Advantages**: No rate limits, processes 8.8M+ contracts efficiently
- **Memory Usage**: ~1GB RAM using DuckDB
- **Best for**: Initial bulk processing of all historical contracts

### 2. Sourcify API (for Incremental Updates)
- **Source**: Sourcify REST API
- **Advantages**: Real-time data, good for ongoing updates
- **Limitations**: Rate limited, not suitable for bulk processing
- **Best for**: Processing new contracts after bulk import

### 3. Direct Database Connection (Advanced)
- **Source**: Direct PostgreSQL connection to Sourcify database
- **Requirements**: Google Cloud deployment or VPN access
- **Best for**: Real-time processing at scale

## Complete Pipeline Example

```bash
# Activate virtual environment
source venv/bin/activate

# Step 1: Test data processing (no credentials needed)
python test_data_processing.py

# Step 2: Set up OLI credentials
export OLI_PRIVATE_KEY="your_private_key_here"

# Step 3: Process contracts and submit to OLI
python -c "
from sourcify_data_processor import SourceifyDataProcessor
from oli_submitter import OLISubmitter
import os

# Initialize both modules
processor = SourceifyDataProcessor()
submitter = OLISubmitter(os.getenv('OLI_PRIVATE_KEY'), is_production=False)

# Process contracts in batches
for batch in processor.process_all_contracts(batch_size=1000):
    successful, total = submitter.submit_batch(batch, submit_onchain=False, delay=1.0)
    print(f'Batch complete: {successful}/{total} successful submissions')
"
```

## Configuration

### Required Environment Variables

- `OLI_PRIVATE_KEY`: Your private key for OLI authentication (wallet must contain ETH on Base)

### Optional Environment Variables

- `USE_PRODUCTION`: Set to "true" for Base Mainnet, "false" for Base Sepolia Testnet (default: false)
- `DEFAULT_CHAIN_ID`: Chain ID to process (default: "1" for Ethereum mainnet)
- `DEFAULT_BATCH_SIZE`: Number of contracts to process in batch mode (default: 100)
- `SUBMISSION_DELAY`: Delay between submissions in seconds (default: 1.0)
- `SOURCIFY_API_URL`: Sourcify API URL (default: https://sourcify.dev/server)

## Virtual Environment Protection

All Python scripts include virtual environment detection and will warn you if not running in a virtual environment:

```bash
# If you run without venv, you'll see:
⚠️  WARNING: Not running in a virtual environment!
   It's recommended to use a virtual environment to avoid dependency conflicts.
   Setup instructions:
   1. python3 -m venv venv
   2. source venv/bin/activate
   3. pip install -r requirements.txt
   4. python test_data_processing.py

Continue anyway? [y/N]:
```

This prevents accidental global package installation and dependency conflicts.

## Submission Types

- **Offchain**: Free submissions stored in OLI database
- **Onchain**: Requires ETH for gas, creates attestations on Base blockchain

## Supported Chains

The tool can process verified contracts from any chain supported by Sourcify. Common chains:
- Ethereum Mainnet (1)
- Polygon (137) 
- Arbitrum (42161)
- Optimism (10)
- And many more...

## Rate Limiting

The tool includes configurable delays between submissions to avoid overwhelming APIs. Adjust `SUBMISSION_DELAY` as needed.

## Error Handling

- Logs all operations with detailed error messages
- Validates tags before submission using OLI SDK
- Graceful handling of missing metadata or creation info
- Continues processing batch even if individual contracts fail

## Development

### Tag Mapping Logic

The tool maps Sourcify contract data to OLI tags as follows:

```python
{
    "source_code_verified": "sourcify",  # Always sourcify
    "is_contract": True,                 # Always true for verified contracts
    "code_language": "solidity",         # Detected from compiler info
    "code_compiler": "solc-0.8.19",     # Extracted from metadata
    "deployment_block": 18500000,        # From creation info API
    "deployment_tx": "0x1234...",        # From creation info API
    "deployer_address": "0x5678..."      # From creation info API
}
```

### API Endpoints Used

- `GET /contracts/full_match/{chainId}` - List verified contracts
- `GET /repository/contracts/full_match/{chainId}/{address}/metadata.json` - Contract metadata
- `GET /contracts/{chainId}/{address}/creation-info` - Deployment information

## License

MIT License