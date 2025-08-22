#!/usr/bin/env python3
"""
SourceifyOLIBridge - Push verified smart contract labels from Sourcify to OLI
Supports multiple data sources: Parquet files, Sourcify API, or direct database connection
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
import requests
import pandas as pd
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import NoCredentialsError
from sqlalchemy import create_engine, text
import pg8000
from google.cloud.sql.connector import Connector, IPTypes
from oli import OLI


class SourceifyOLIBridge:
    """Bridge to push Sourcify verified contracts to OLI with specific tags."""
    
    def __init__(self, private_key: str, is_production: bool = True):
        """
        Initialize the bridge with OLI configuration.
        
        Args:
            private_key: Private key for OLI authentication
            is_production: True for Base Mainnet, False for Base Sepolia Testnet
        """
        self.oli = OLI(private_key=private_key, is_production=is_production)
        self.logger = self._setup_logger()
        self.sourcify_api = "https://sourcify.dev/server"
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('SourceifyOLIBridge')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    def get_verified_contracts(self, chain_id: str, limit: int = 100) -> List[Dict]:
        """
        Fetch verified contracts from Sourcify API.
        
        Args:
            chain_id: Chain ID (e.g., "1" for Ethereum mainnet)
            limit: Maximum number of contracts to fetch
            
        Returns:
            List of verified contract data
        """
        try:
            # Get contracts from Sourcify repository
            url = f"{self.sourcify_api}/contracts/full_match/{chain_id}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            contracts = response.json()
            return contracts[:limit] if len(contracts) > limit else contracts
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch contracts from Sourcify: {e}")
            return []
    
    def get_contract_metadata(self, address: str, chain_id: str) -> Optional[Dict]:
        """
        Get detailed metadata for a specific contract.
        
        Args:
            address: Contract address
            chain_id: Chain ID
            
        Returns:
            Contract metadata or None if not found
        """
        try:
            # Get contract source and metadata
            url = f"{self.sourcify_api}/repository/contracts/full_match/{chain_id}/{address}/metadata.json"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException:
            self.logger.warning(f"Could not fetch metadata for {address} on chain {chain_id}")
            return None
    
    def get_contract_creation_info(self, address: str, chain_id: str) -> Optional[Dict]:
        """
        Get contract creation information (deployment block, tx, deployer).
        
        Args:
            address: Contract address
            chain_id: Chain ID
            
        Returns:
            Creation info or None if not available
        """
        try:
            # Try to get creation info from Sourcify
            url = f"{self.sourcify_api}/contracts/{chain_id}/{address}/creation-info"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException:
            self.logger.warning(f"Could not fetch creation info for {address} on chain {chain_id}")
            return None
    
    def map_to_oli_tags(self, address: str, chain_id: str, metadata: Dict, creation_info: Optional[Dict] = None) -> Dict[str, any]:
        """
        Map Sourcify contract data to OLI tags.
        
        Args:
            address: Contract address
            chain_id: Chain ID
            metadata: Contract metadata from Sourcify
            creation_info: Contract creation information
            
        Returns:
            Dictionary of OLI tags
        """
        tags = {}
        
        # source_code_verified - always 'sourcify' since we're getting from Sourcify
        tags["source_code_verified"] = "sourcify"
        
        # is_contract - always True for verified contracts
        tags["is_contract"] = True
        
        # code_language - extract from compiler info
        compiler = metadata.get("compiler", {})
        if "solc" in str(compiler).lower():
            tags["code_language"] = "solidity"
        elif "vyper" in str(compiler).lower():
            tags["code_language"] = "vyper"
        # Add other languages as needed
        
        # code_compiler - compiler version
        if compiler:
            version = compiler.get("version", "")
            if version:
                tags["code_compiler"] = f"solc-{version}" if "solc" in str(compiler).lower() else str(compiler)
        
        # Add creation info if available
        if creation_info:
            if "blockNumber" in creation_info:
                tags["deployment_block"] = int(creation_info["blockNumber"])
            
            if "txHash" in creation_info:
                tx_hash = creation_info["txHash"]
                if len(tx_hash) == 66:  # Valid tx hash format
                    tags["deployment_tx"] = tx_hash
            
            if "deployer" in creation_info:
                deployer = creation_info["deployer"]
                if len(deployer) == 42:  # Valid address format
                    tags["deployer_address"] = deployer
        
        return tags
    
    def process_contract(self, address: str, chain_id: str, submit_onchain: bool = False) -> bool:
        """
        Process a single contract and submit its tags to OLI.
        
        Args:
            address: Contract address
            chain_id: Chain ID (numeric string)
            submit_onchain: Whether to submit onchain (requires gas) or offchain
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Processing contract {address} on chain {chain_id}")
            
            # Get contract metadata
            metadata = self.get_contract_metadata(address, chain_id)
            if not metadata:
                self.logger.warning(f"No metadata found for {address}")
                return False
            
            # Get creation info
            creation_info = self.get_contract_creation_info(address, chain_id)
            
            # Map to OLI tags
            tags = self.map_to_oli_tags(address, chain_id, metadata, creation_info)
            
            if not tags:
                self.logger.warning(f"No tags generated for {address}")
                return False
            
            # Format chain_id for OLI (eip155:chain_id)
            oli_chain_id = f"eip155:{chain_id}"
            
            # Validate tags
            is_valid = self.oli.validate_label_correctness(address, oli_chain_id, tags)
            if not is_valid:
                self.logger.error(f"Tags validation failed for {address}: {tags}")
                return False
            
            # Submit tags
            if submit_onchain:
                tx_hash, uid = self.oli.submit_onchain_label(address, oli_chain_id, tags)
                self.logger.info(f"Onchain submission successful for {address}: tx={tx_hash}, uid={uid}")
            else:
                response = self.oli.submit_offchain_label(address, oli_chain_id, tags)
                self.logger.info(f"Offchain submission successful for {address}: {response}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing contract {address}: {e}")
            return False
    
    def process_batch(self, chain_id: str, limit: int = 100, submit_onchain: bool = False, delay: float = 1.0) -> Tuple[int, int]:
        """
        Process a batch of verified contracts from Sourcify.
        
        Args:
            chain_id: Chain ID to process
            limit: Maximum number of contracts to process
            submit_onchain: Whether to submit onchain or offchain
            delay: Delay between submissions (seconds)
            
        Returns:
            Tuple of (successful_count, total_count)
        """
        self.logger.info(f"Starting batch processing for chain {chain_id}, limit={limit}")
        
        # Get verified contracts
        contracts = self.get_verified_contracts(chain_id, limit)
        if not contracts:
            self.logger.warning(f"No contracts found for chain {chain_id}")
            return 0, 0
        
        successful = 0
        total = len(contracts)
        
        for i, address in enumerate(contracts):
            self.logger.info(f"Processing {i+1}/{total}: {address}")
            
            if self.process_contract(address, chain_id, submit_onchain):
                successful += 1
            
            # Rate limiting
            if i < total - 1:  # Don't delay after the last item
                time.sleep(delay)
        
        self.logger.info(f"Batch processing complete: {successful}/{total} successful")
        return successful, total


def main():
    """Example usage of SourceifyOLIBridge."""
    
    # Get private key from environment
    private_key = os.getenv('OLI_PRIVATE_KEY')
    if not private_key:
        print("Error: OLI_PRIVATE_KEY environment variable not set")
        return
    
    # Initialize bridge
    bridge = SourceifyOLIBridge(private_key, is_production=False)  # Use testnet for testing
    
    # Process some contracts from Ethereum mainnet
    successful, total = bridge.process_batch(
        chain_id="1",  # Ethereum mainnet
        limit=5,       # Process 5 contracts
        submit_onchain=False,  # Use offchain (free) submissions
        delay=2.0      # 2 second delay between submissions
    )
    
    print(f"Processed {successful}/{total} contracts successfully")


if __name__ == "__main__":
    main()