#!/usr/bin/env python3
"""
OLI Submitter Module

Handles submission of contract labels to the Open Labels Initiative (OLI) platform.
Separated from data processing for modularity and testing.
"""

import json
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple
import pandas as pd
from dotenv import load_dotenv
from oli import OLI


def check_virtual_environment():
    """Check if running in a virtual environment and warn if not."""
    # Check if we're in a virtual environment
    in_venv = (
        hasattr(sys, 'real_prefix') or  # virtualenv
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) or  # venv
        os.environ.get('VIRTUAL_ENV') is not None  # env variable
    )
    
    if not in_venv:
        print("⚠️  WARNING: Not running in a virtual environment!")
        print("   It's recommended to use a virtual environment to avoid dependency conflicts.")
        print("   Setup instructions:")
        print("   1. python3 -m venv venv")
        print("   2. source venv/bin/activate") 
        print("   3. pip install -r requirements.txt")
        print("   4. python", sys.argv[0])
        print()
        
        # Ask user if they want to continue
        try:
            response = input("Continue anyway? [y/N]: ").lower().strip()
            if response not in ['y', 'yes']:
                print("Exiting. Please setup virtual environment first.")
                sys.exit(1)
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(1)


class OLISubmitter:
    """Submit contract labels to OLI platform."""
    
    def __init__(self, private_key: str, is_production: bool = False):
        """
        Initialize OLI submitter.
        
        Args:
            private_key: Private key for OLI authentication
            is_production: True for Base Mainnet, False for Base Sepolia Testnet
        """
        self.oli = OLI(private_key=private_key, is_production=is_production)
        self.is_production = is_production
        self.logger = self._setup_logger()

        # Log the attestation address for verification
        network = "Base Mainnet" if is_production else "Base Sepolia Testnet"
        self.logger.info(f"OLI Client initialized for {network}")
        self.logger.info(f"Attestation address: {self.oli.address}")
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('OLISubmitter')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
        
    def format_chain_id_for_oli(self, chain_id: int) -> str:
        """
        Format chain ID for OLI (eip155 format).
        
        Args:
            chain_id: Numeric chain ID
            
        Returns:
            Chain ID in eip155:chainId format
        """
        return f"eip155:{chain_id}"
        
    def prepare_oli_tags(self, contract_data: Dict) -> Dict:
        """
        Convert contract data to OLI tags format.
        
        Args:
            contract_data: Dictionary with contract information
            
        Returns:
            Dictionary of OLI tags
        """
        tags = {}
        
        # Required tags
        tags["source_code_verified"] = "sourcify"
        tags["is_contract"] = True
        
        # Optional tags (only include if data exists)
        if contract_data.get('code_language') and pd.notna(contract_data['code_language']):
            # Map language names to OLI format
            language = str(contract_data['code_language']).lower()
            if language == 'solidity':
                tags["code_language"] = "solidity"
            elif language == 'vyper':
                tags["code_language"] = "vyper"
            elif language in ['yul', 'fe', 'huff']:
                tags["code_language"] = language
                
        if contract_data.get('code_compiler') and pd.notna(contract_data['code_compiler']):
            tags["code_compiler"] = str(contract_data['code_compiler'])
            
        if contract_data.get('deployment_block') and pd.notna(contract_data['deployment_block']):
            try:
                tags["deployment_block"] = int(contract_data['deployment_block'])
            except (ValueError, TypeError):
                pass  # Skip if can't convert to int
                
        if contract_data.get('deployment_tx') and pd.notna(contract_data['deployment_tx']):
            tx_hash = str(contract_data['deployment_tx'])
            if len(tx_hash) == 66 and tx_hash.startswith('0x'):  # Valid tx hash format
                tags["deployment_tx"] = tx_hash
                
        if contract_data.get('deployer_address') and pd.notna(contract_data['deployer_address']):
            deployer = str(contract_data['deployer_address'])
            if len(deployer) == 42 and deployer.startswith('0x'):  # Valid address format
                tags["deployer_address"] = deployer
                
        if contract_data.get('contract_name') and pd.notna(contract_data['contract_name']):
            contract_name = str(contract_data['contract_name']).strip()
            if contract_name:  # Only include if non-empty after stripping
                tags["contract_name"] = contract_name
                
        return tags
        
    def validate_submission(self, address: str, chain_id: str, tags: Dict) -> bool:
        """
        Validate a label submission before sending to OLI.
        
        Args:
            address: Contract address
            chain_id: Chain ID in eip155 format
            tags: Dictionary of OLI tags
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Basic validation
            if not address or not address.startswith('0x') or len(address) != 42:
                self.logger.error(f"Invalid address format: {address}")
                return False
                
            if not chain_id or not chain_id.startswith('eip155:'):
                self.logger.error(f"Invalid chain ID format: {chain_id}")
                return False
                
            if not tags or len(tags) < 2:  # At minimum should have source_code_verified and is_contract
                self.logger.error(f"Insufficient tags: {tags}")
                return False
                
            # Use OLI's validation
            is_valid = self.oli.validate_label_correctness(address, chain_id, tags)
            if not is_valid:
                self.logger.error(f"OLI validation failed for {address}: {tags}")
                
            return is_valid
            
        except Exception as e:
            self.logger.error(f"Validation error for {address}: {e}")
            return False
            
    def submit_contract(self, contract_data: Dict, submit_onchain: bool = False) -> bool:
        """
        Submit a single contract to OLI.
        
        Args:
            contract_data: Dictionary with contract information
            submit_onchain: Whether to submit onchain (requires gas) or offchain
            
        Returns:
            True if submission successful, False otherwise
        """
        try:
            address = str(contract_data['address'])
            chain_id = self.format_chain_id_for_oli(int(contract_data['chain_id']))
            tags = self.prepare_oli_tags(contract_data)
            
            # Validate before submission
            if not self.validate_submission(address, chain_id, tags):
                return False
                
            # Submit to OLI
            if submit_onchain:
                tx_hash, uid = self.oli.submit_onchain_label(address, chain_id, tags)
                self.logger.info(f"Onchain submission successful for {address}: tx={tx_hash}, uid={uid}")
            else:
                response = self.oli.submit_offchain_label(address, chain_id, tags)
                self.logger.info(f"Offchain submission successful for {address}")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Submission failed for contract {contract_data.get('address', 'unknown')}: {e}")
            return False
            
    def submit_batch(self, contracts_df: pd.DataFrame, submit_onchain: bool = False, 
                    delay: float = 1.0) -> Tuple[int, int]:
        """
        Submit a batch of contracts to OLI.
        
        Args:
            contracts_df: DataFrame with contract data
            submit_onchain: Whether to submit onchain or offchain
            delay: Delay between submissions (seconds)
            
        Returns:
            Tuple of (successful_count, total_count)
        """
        successful = 0
        total = len(contracts_df)
        
        self.logger.info(f"Starting batch submission: {total} contracts "
                        f"({'onchain' if submit_onchain else 'offchain'})")
        
        if submit_onchain and total > 1:
            # Use efficient batch onchain submission for multiple contracts
            return self._submit_batch_onchain(contracts_df)
        elif not submit_onchain and total > 1:
            # Use parallel processing for offchain submissions
            return self._submit_batch_parallel_offchain(contracts_df, delay)
        else:
            # Use individual submissions for single contracts
            return self._submit_batch_individual(contracts_df, submit_onchain, delay)
    
    def _submit_batch_onchain(self, contracts_df: pd.DataFrame) -> Tuple[int, int]:
        """
        Submit batch of contracts using OLI's multi-onchain submission method.
        
        Args:
            contracts_df: DataFrame with contract data
            
        Returns:
            Tuple of (successful_count, total_count)
        """
        total = len(contracts_df)
        
        # Prepare labels for batch submission
        labels = []
        valid_contracts = 0
        
        for idx, row in contracts_df.iterrows():
            try:
                address = str(row['address'])
                chain_id = self.format_chain_id_for_oli(int(row['chain_id']))
                tags = self.prepare_oli_tags(row.to_dict())
                
                # Validate before adding to batch
                if self.validate_submission(address, chain_id, tags):
                    labels.append({
                        'address': address,
                        'chain_id': chain_id, 
                        'tags': tags
                    })
                    valid_contracts += 1
                else:
                    self.logger.warning(f"Skipping invalid contract {address}")
                    
            except Exception as e:
                self.logger.error(f"Error preparing contract at index {idx}: {e}")
        
        if not labels:
            self.logger.warning("No valid contracts to submit")
            return 0, total
            
        self.logger.info(f"Submitting {len(labels)} contracts in single onchain batch")
        
        try:
            # Submit entire batch in one transaction
            tx_hash, uids = self.oli.submit_multi_onchain_labels(labels)
            successful = len(uids) if uids else 0
            
            self.logger.info(f"Batch onchain submission successful: tx={tx_hash}")
            self.logger.info(f"Generated {successful} UIDs for {len(labels)} labels")
            
            return successful, total
            
        except Exception as e:
            self.logger.error(f"Batch onchain submission failed: {e}")
            # Fallback to individual submissions
            self.logger.info("Falling back to individual submissions...")
            return self._submit_batch_individual(contracts_df, True, 1.0)
    
    def _submit_batch_parallel_offchain(self, contracts_df: pd.DataFrame, delay: float) -> Tuple[int, int]:
        """
        Submit batch of contracts using parallel offchain submissions.
        
        Args:
            contracts_df: DataFrame with contract data
            delay: Delay between batch chunks (not individual submissions)
            
        Returns:
            Tuple of (successful_count, total_count)
        """
        import concurrent.futures
        import time
        
        total = len(contracts_df)
        # Configurable max workers (default 10, reasonable for most APIs)
        max_workers = min(
            int(os.getenv('MAX_PARALLEL_WORKERS', '10')), 
            total
        )
        
        self.logger.info(f"Submitting {total} contracts with {max_workers} parallel workers")
        
        successful = 0
        failed_contracts = []
        
        def submit_single_offchain(contract_data):
            """Submit a single contract offchain."""
            try:
                return self.submit_contract(contract_data, submit_onchain=False)
            except Exception as e:
                self.logger.error(f"Parallel submission error for {contract_data.get('address', 'unknown')}: {e}")
                return False
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all contracts in parallel
            future_to_contract = {
                executor.submit(submit_single_offchain, row.to_dict()): idx
                for idx, row in contracts_df.iterrows()
            }
            
            # Process completed submissions
            for future in concurrent.futures.as_completed(future_to_contract):
                contract_idx = future_to_contract[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed_contracts.append(contract_idx)
                except Exception as e:
                    failed_contracts.append(contract_idx)
                    self.logger.error(f"Exception in parallel submission: {e}")
                
                # Progress reporting
                completed = successful + len(failed_contracts)
                if completed % 50 == 0 or completed == total:
                    self.logger.info(f"Progress: {completed}/{total} contracts "
                                   f"({successful} successful, {len(failed_contracts)} failed)")
        
        duration = time.time() - start_time
        rate = total / duration if duration > 0 else 0
        
        self.logger.info(f"Parallel offchain submission complete: {successful}/{total} successful")
        self.logger.info(f"Processing rate: {rate:.1f} contracts/sec")
        
        if failed_contracts and len(failed_contracts) < total * 0.1:  # If < 10% failed, retry them
            self.logger.info(f"Retrying {len(failed_contracts)} failed contracts sequentially...")
            failed_df = contracts_df.iloc[failed_contracts]
            retry_successful, retry_total = self._submit_batch_individual(
                failed_df, submit_onchain=False, delay=delay
            )
            successful += retry_successful
        
        return successful, total
    
    def _submit_batch_individual(self, contracts_df: pd.DataFrame, submit_onchain: bool, 
                                delay: float) -> Tuple[int, int]:
        """
        Submit contracts individually (used for offchain or fallback).
        
        Args:
            contracts_df: DataFrame with contract data
            submit_onchain: Whether to submit onchain
            delay: Delay between submissions
            
        Returns:
            Tuple of (successful_count, total_count)  
        """
        successful = 0
        total = len(contracts_df)
        
        for idx, row in contracts_df.iterrows():
            try:
                if self.submit_contract(row.to_dict(), submit_onchain):
                    successful += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing contract at index {idx}: {e}")
                
            # Rate limiting
            if idx < total - 1:  # Don't delay after the last item
                time.sleep(delay)
                
            # Progress reporting
            if (idx + 1) % 100 == 0:
                self.logger.info(f"Progress: {idx + 1}/{total} contracts processed "
                               f"({successful} successful)")
        
        self.logger.info(f"Individual submission complete: {successful}/{total} successful")
        return successful, total
        
    def test_single_submission(self, address: str, chain_id: int, 
                             sample_tags: Optional[Dict] = None) -> bool:
        """
        Test submission with a single contract (useful for testing).
        
        Args:
            address: Contract address to test
            chain_id: Chain ID
            sample_tags: Optional custom tags, otherwise uses defaults
            
        Returns:
            True if test successful
        """
        try:
            oli_chain_id = self.format_chain_id_for_oli(chain_id)
            
            if sample_tags:
                tags = sample_tags
            else:
                # Default test tags
                tags = {
                    "source_code_verified": "sourcify",
                    "is_contract": True,
                    "code_language": "solidity",
                    "code_compiler": "solc-0.8.19"
                }
                
            self.logger.info(f"Testing submission for {address} on chain {oli_chain_id}")
            self.logger.info(f"Tags: {json.dumps(tags, indent=2)}")
            
            # Validate
            is_valid = self.validate_submission(address, oli_chain_id, tags)
            if not is_valid:
                self.logger.error("Validation failed")
                return False
                
            # Test offchain submission
            response = self.oli.submit_offchain_label(address, oli_chain_id, tags)
            self.logger.info(f"Test submission successful: {response}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Test submission failed: {e}")
            return False
            
    def get_submission_stats(self, contracts_df: pd.DataFrame) -> Dict:
        """
        Analyze contracts DataFrame and return submission statistics.
        
        Args:
            contracts_df: DataFrame with contract data
            
        Returns:
            Dictionary with statistics
        """
        total_contracts = len(contracts_df)
        
        # Count contracts with each tag type
        stats = {
            'total_contracts': total_contracts,
            'unique_chains': contracts_df['chain_id'].nunique(),
            'contracts_with_deployment_tx': contracts_df['deployment_tx'].notna().sum(),
            'contracts_with_deployment_block': contracts_df['deployment_block'].notna().sum(),
            'contracts_with_deployer': contracts_df['deployer_address'].notna().sum(),
            'contracts_with_language': contracts_df['code_language'].notna().sum(),
            'contracts_with_compiler': contracts_df['code_compiler'].notna().sum(),
            'chains': contracts_df['chain_id'].value_counts().head(10).to_dict(),
            'languages': contracts_df['code_language'].value_counts().to_dict() if 'code_language' in contracts_df.columns else {}
        }
        
        return stats


def main():
    """Example usage of OLISubmitter."""
    check_virtual_environment()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Example with environment variable
    private_key = os.getenv('OLI_PRIVATE_KEY')
    if not private_key:
        print("Please set OLI_PRIVATE_KEY environment variable")
        return
        
    submitter = OLISubmitter(private_key, is_production=False)  # Use testnet
    
    # Test with a sample contract
    test_address = "0x9438b8B447179740cD97869997a2FCc9b4AA63a2"
    test_chain_id = 1  # Ethereum mainnet
    
    print("Testing OLI submission...")
    success = submitter.test_single_submission(test_address, test_chain_id)
    print(f"Test {'passed' if success else 'failed'}")


if __name__ == "__main__":
    main()