#!/usr/bin/env python3
"""
Example usage of SourcifyOLIBridge
"""

import os
from dotenv import load_dotenv
from sourcify_oli_bridge import SourcifyOLIBridge

def main():
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    private_key = os.getenv('OLI_PRIVATE_KEY')
    use_production = os.getenv('USE_PRODUCTION', 'false').lower() == 'true'
    chain_id = os.getenv('DEFAULT_CHAIN_ID', '1')
    batch_size = int(os.getenv('DEFAULT_BATCH_SIZE', '10'))
    delay = float(os.getenv('SUBMISSION_DELAY', '1.0'))
    
    if not private_key:
        print("Error: Please set OLI_PRIVATE_KEY in your .env file")
        print("Copy .env.example to .env and fill in your private key")
        return
    
    print(f"Initializing SourcifyOLIBridge...")
    print(f"- Production mode: {use_production}")
    print(f"- Chain ID: {chain_id}")
    print(f"- Batch size: {batch_size}")
    print(f"- Delay: {delay}s")
    
    # Initialize bridge
    bridge = SourcifyOLIBridge(private_key, is_production=use_production)
    
    # Example 1: Process a single contract
    print("\n=== Example 1: Single Contract ===")
    
    # You can specify a known verified contract address
    test_address = "0x9438b8B447179740cD97869997a2FCc9b4AA63a2"  # Example address
    success = bridge.process_contract(
        address=test_address,
        chain_id=chain_id,
        submit_onchain=False  # Use offchain for testing
    )
    print(f"Single contract processing: {'Success' if success else 'Failed'}")
    
    # Example 2: Process a batch of contracts
    print("\n=== Example 2: Batch Processing ===")
    
    successful, total = bridge.process_batch(
        chain_id=chain_id,
        limit=batch_size,
        submit_onchain=False,  # Use offchain for testing
        delay=delay
    )
    
    print(f"\nBatch processing complete!")
    print(f"Successfully processed: {successful}/{total} contracts")
    
    if successful > 0:
        print(f"\nLabels have been submitted to OLI {'(production)' if use_production else '(testnet)'}")
        print("You can verify the submissions in the OLI dashboard")


if __name__ == "__main__":
    main()