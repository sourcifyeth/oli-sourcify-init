# Sourcify OLI Bridge

Push verified smart contract labels from Sourcify to the Open Labels Initiative (OLI) using their Python SDK.

## Overview

This tool fetches verified smart contracts from Sourcify and submits them to OLI with the following tags:
- `source_code_verified`: "sourcify"
- `code_language`: Detected from compiler (solidity, vyper, etc.)
- `code_compiler`: Compiler version used
- `deployment_block`: Block number where contract was deployed
- `deployment_tx`: Transaction hash that deployed the contract
- `is_contract`: Always true for verified contracts
- `deployer_address`: Address that deployed the contract

## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your OLI private key
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

## Usage

### Basic Example

```python
from sourcify_oli_bridge import SourceifyOLIBridge

# Initialize bridge
bridge = SourceifyOLIBridge(
    private_key="your_private_key_here",
    is_production=False  # Use testnet for testing
)

# Process a single contract
success = bridge.process_contract(
    address="0x1234...",
    chain_id="1",  # Ethereum mainnet
    submit_onchain=False  # Offchain is free
)

# Process multiple contracts
successful, total = bridge.process_batch(
    chain_id="1",
    limit=10,
    submit_onchain=False,
    delay=2.0
)
```

### Running the Example

```bash
python example.py
```

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