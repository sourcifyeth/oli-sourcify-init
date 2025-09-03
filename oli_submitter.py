#!/usr/bin/env python3
"""
OLI Submitter Module

Handles submission of contract labels to the Open Labels Initiative (OLI) platform.
Separated from data processing for modularity and testing.
"""

import json
import logging
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path
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
    """Submit contract labels to OLI platform with persistent state tracking."""
    
    def __init__(self, private_key: str, is_production: bool = False, state_dir: str = "./oli_state"):
        """
        Initialize OLI submitter.
        
        Args:
            private_key: Private key for OLI authentication
            is_production: True for Base Mainnet, False for Base Sepolia Testnet
            state_dir: Directory to store submission state database
        """
        self.oli = OLI(private_key=private_key, is_production=is_production)
        self.is_production = is_production
        self.logger = self._setup_logger()
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(exist_ok=True)
        
        # Initialize state database
        self.db_path = self.state_dir / "submissions.db"
        self._init_state_db()
        
        # Initialize CSV file for failed contracts (immediate logging)
        self.failed_csv_path = self.state_dir / "failed_contracts_live.csv"
        self._init_failed_csv()
        
        # Graceful shutdown handling
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Session counters
        self.session_stats = {
            'total_skipped': 0,
            'total_processed': 0,
            'session_start': time.time()
        }

        # Log the attestation address for verification
        network = "Base Mainnet" if is_production else "Base Sepolia Testnet"
        self.logger.info(f"OLI Client initialized for {network}")
        self.logger.info(f"Attestation address: {self.oli.address}")
        self.logger.info(f"State tracking: {self.db_path}")
        
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
    
    def _init_state_db(self):
        """Initialize SQLite database for tracking submission state."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS submissions (
                    address TEXT,
                    chain_id INTEGER,
                    status TEXT, -- 'success', 'failed', 'pending'
                    timestamp TEXT,
                    tx_hash TEXT, -- for onchain submissions
                    error_message TEXT,
                    tags_json TEXT, -- JSON representation of tags
                    PRIMARY KEY (address, chain_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status 
                ON submissions(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON submissions(timestamp)
            """)
    
    def _init_failed_csv(self):
        """Initialize CSV file for immediate failed contract logging."""
        if not self.failed_csv_path.exists():
            # Create CSV with headers
            with open(self.failed_csv_path, 'w') as f:
                f.write("timestamp,address,chain_id,error_message,tags_json\n")
    
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown signals."""
        signal_name = {signal.SIGINT: 'SIGINT', signal.SIGTERM: 'SIGTERM'}.get(signum, str(signum))
        self.logger.info(f"Received {signal_name} - initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def _is_already_submitted(self, address: str, chain_id: int) -> bool:
        """Check if a contract has already been successfully submitted."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT status FROM submissions WHERE address = ? AND chain_id = ? AND status = 'success'",
                (address.lower(), chain_id)
            )
            return cursor.fetchone() is not None
    
    def _record_submission(self, address: str, chain_id: int, status: str, 
                          tags: Dict, tx_hash: Optional[str] = None, 
                          error_message: Optional[str] = None):
        """Record a submission attempt in the state database and immediately log failures to CSV."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        tags_json = json.dumps(tags, sort_keys=True)
        
        # Record in database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO submissions 
                (address, chain_id, status, timestamp, tx_hash, error_message, tags_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                address.lower(), 
                chain_id, 
                status, 
                timestamp,
                tx_hash, 
                error_message, 
                tags_json
            ))
        
        # Immediately log failed contracts to CSV
        if status == 'failed':
            self._log_failed_to_csv(timestamp, address, chain_id, error_message, tags_json)
    
    def _log_failed_to_csv(self, timestamp: str, address: str, chain_id: int, 
                          error_message: str, tags_json: str):
        """Immediately log failed contract to CSV file."""
        try:
            # Escape commas and quotes in error message and tags for CSV
            error_escaped = (error_message or '').replace('"', '""').replace('\n', ' ')
            tags_escaped = tags_json.replace('"', '""')
            
            with open(self.failed_csv_path, 'a') as f:
                f.write(f'{timestamp},"{address}",{chain_id},"{error_escaped}","{tags_escaped}"\n')
                f.flush()  # Force write to disk immediately
        except Exception as e:
            self.logger.error(f"Failed to log to CSV: {e}")
    
    def get_submission_stats(self, contracts_df: Optional[pd.DataFrame] = None) -> Dict:
        """
        Get statistics from the submission database or analyze contracts DataFrame.
        
        Args:
            contracts_df: Optional DataFrame to analyze (legacy compatibility)
            
        Returns:
            Dictionary with statistics
        """
        if contracts_df is not None:
            # Legacy mode - analyze DataFrame (keep for backward compatibility)
            total_contracts = len(contracts_df)
            
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
        else:
            # New mode - get submission database statistics
            with sqlite3.connect(self.db_path) as conn:
                # Overall stats
                cursor = conn.execute("SELECT status, COUNT(*) FROM submissions GROUP BY status")
                status_counts = dict(cursor.fetchall())
                
                # Total count
                cursor = conn.execute("SELECT COUNT(*) FROM submissions")
                total = cursor.fetchone()[0]
                
                # Recent submissions (last hour)
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM submissions 
                    WHERE timestamp > datetime('now', '-1 hour')
                """)
                recent = cursor.fetchone()[0]
                
                return {
                    'total_submissions': total,
                    'successful': status_counts.get('success', 0),
                    'failed': status_counts.get('failed', 0),
                    'pending': status_counts.get('pending', 0),
                    'recent_hour': recent,
                    'success_rate': status_counts.get('success', 0) / total * 100 if total > 0 else 0
                }
    
    def get_session_stats(self) -> Dict:
        """Get statistics for the current session."""
        session_duration = time.time() - self.session_stats['session_start']
        return {
            'session_duration_hours': session_duration / 3600,
            'total_skipped': self.session_stats['total_skipped'],
            'total_processed': self.session_stats['total_processed'],
            'skip_rate': self.session_stats['total_skipped'] / (self.session_stats['total_skipped'] + self.session_stats['total_processed']) * 100 
                        if (self.session_stats['total_skipped'] + self.session_stats['total_processed']) > 0 else 0
        }
    
    def filter_unsubmitted_contracts(self, contracts_df: pd.DataFrame) -> pd.DataFrame:
        """Filter out contracts that have already been successfully submitted."""
        if len(contracts_df) == 0:
            return contracts_df
            
        # Get all successfully submitted contracts
        with sqlite3.connect(self.db_path) as conn:
            submitted_df = pd.read_sql_query("""
                SELECT LOWER(address) as address, chain_id 
                FROM submissions 
                WHERE status = 'success'
            """, conn)
        
        if len(submitted_df) == 0:
            self.logger.info("No previously submitted contracts found - processing all")
            return contracts_df
        
        # Create composite key for matching
        contracts_df['composite_key'] = contracts_df['address'].str.lower() + '_' + contracts_df['chain_id'].astype(str)
        submitted_df['composite_key'] = submitted_df['address'] + '_' + submitted_df['chain_id'].astype(str)
        
        # Filter out already submitted
        unsubmitted = contracts_df[~contracts_df['composite_key'].isin(submitted_df['composite_key'])].copy()
        unsubmitted.drop('composite_key', axis=1, inplace=True)
        
        skipped = len(contracts_df) - len(unsubmitted)
        self.session_stats['total_skipped'] += skipped
        
        if skipped > 0:
            self.logger.info(f"Skipping {skipped:,} already submitted contracts, processing {len(unsubmitted):,} new ones")
            self.logger.info(f"Session totals: {self.session_stats['total_skipped']:,} skipped, "
                           f"{self.session_stats['total_processed']:,} processed")
            
            # Log detailed info for smaller batches
            if skipped <= 10:
                # Log individual skipped contracts for small numbers
                skipped_contracts = contracts_df[contracts_df['composite_key'].isin(submitted_df['composite_key'])]
                for _, row in skipped_contracts.head(10).iterrows():
                    self.logger.debug(f"Already submitted: {row['address']} on chain {row['chain_id']}")
            elif skipped <= 1000:
                # Log summary for medium numbers
                self.logger.debug(f"Batch skipped {skipped} previously submitted contracts")
        else:
            self.logger.debug("No previously submitted contracts found in this batch")
        
        return unsubmitted
        
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
        Submit a single contract to OLI with state tracking.
        
        Args:
            contract_data: Dictionary with contract information
            submit_onchain: Whether to submit onchain (requires gas) or offchain
            
        Returns:
            True if submission successful, False otherwise
        """
        address = str(contract_data['address'])
        chain_id_int = int(contract_data['chain_id'])
        chain_id = self.format_chain_id_for_oli(chain_id_int)
        
        # Check if already submitted successfully
        if self._is_already_submitted(address, chain_id_int):
            self.logger.debug(f"Contract {address} on chain {chain_id_int} already submitted - skipping")
            return True  # Already submitted, count as success
        
        try:
            tags = self.prepare_oli_tags(contract_data)
            
            # Record as pending
            self._record_submission(address, chain_id_int, 'pending', tags)
            
            # Validate before submission
            if not self.validate_submission(address, chain_id, tags):
                self._record_submission(address, chain_id_int, 'failed', tags, error_message="Validation failed")
                return False
                
            # Submit to OLI
            tx_hash = None
            if submit_onchain:
                tx_hash, uid = self.oli.submit_onchain_label(address, chain_id, tags)
                self.logger.info(f"Onchain submission successful for {address}: tx={tx_hash}, uid={uid}")
            else:
                response = self.oli.submit_offchain_label(address, chain_id, tags)
                self.logger.info(f"Offchain submission successful for {address}")
                
            # Record success
            self._record_submission(address, chain_id_int, 'success', tags, tx_hash=tx_hash)
            return True
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Submission failed for contract {address}: {error_msg}")
            self._record_submission(address, chain_id_int, 'failed', tags, error_message=error_msg)
            return False
            
    def submit_batch(self, contracts_df: pd.DataFrame, submit_onchain: bool = False, 
                    delay: float = 1.0) -> Tuple[int, int]:
        """
        Submit a batch of contracts to OLI with duplicate filtering.
        
        Args:
            contracts_df: DataFrame with contract data
            submit_onchain: Whether to submit onchain or offchain
            delay: Delay between submissions (seconds)
            
        Returns:
            Tuple of (successful_count, total_count)
        """
        original_total = len(contracts_df)
        
        # Filter out already submitted contracts
        contracts_df = self.filter_unsubmitted_contracts(contracts_df)
        total = len(contracts_df)
        already_submitted = original_total - total
        
        self.logger.info(f"Starting batch submission: {total} new contracts, {already_submitted} already submitted "
                        f"({'onchain' if submit_onchain else 'offchain'})")
        
        if total == 0:
            return already_submitted, original_total  # All were already submitted
        
        # Process remaining contracts
        if submit_onchain and total > 1:
            # Use efficient batch onchain submission for multiple contracts
            successful, processed = self._submit_batch_onchain(contracts_df)
        elif not submit_onchain and total > 1:
            # Use parallel processing for offchain submissions
            successful, processed = self._submit_batch_parallel_offchain(contracts_df, delay)
        else:
            # Use individual submissions for single contracts
            successful, processed = self._submit_batch_individual(contracts_df, submit_onchain, delay)
        
        # Update session stats
        self.session_stats['total_processed'] += processed
        
        return successful + already_submitted, original_total
    
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
        
        self.logger.info(f"Starting parallel offchain submission: {total} contracts with {max_workers} workers")
        
        successful = 0
        failed_contracts = []
        
        def submit_single_offchain(contract_data):
            """Submit a single contract offchain."""
            try:
                # Check for shutdown signal before processing
                if self.shutdown_requested:
                    self.logger.info("Shutdown requested - stopping submission")
                    return False
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
            
            # Process completed submissions with speed tracking
            for future in concurrent.futures.as_completed(future_to_contract):
                # Check for shutdown signal
                if self.shutdown_requested:
                    self.logger.info("Shutdown requested - cancelling remaining submissions")
                    # Cancel remaining futures
                    for remaining_future in future_to_contract:
                        remaining_future.cancel()
                    break
                
                contract_idx = future_to_contract[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed_contracts.append(contract_idx)
                except Exception as e:
                    failed_contracts.append(contract_idx)
                    self.logger.error(f"Exception in parallel submission: {e}")
                
                # Progress reporting with speed tracking
                completed = successful + len(failed_contracts)
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                
                # Log progress at regular intervals
                progress_interval = min(max(total // 10, 1), 100)
                if completed % progress_interval == 0 or completed == total:
                    self.logger.info(f"Progress: {completed}/{total} ({completed/total*100:.1f}%) - "
                                   f"{rate:.1f} submissions/sec ({successful} successful, {len(failed_contracts)} failed)")
                    
                    # Check for shutdown after progress logging
                    if self.shutdown_requested:
                        self.logger.info("Shutdown requested during batch - stopping gracefully")
                        break
        
        duration = time.time() - start_time
        final_rate = total / duration if duration > 0 else 0
        
        self.logger.info(f"Parallel offchain submission complete: {successful}/{total} successful in {duration:.2f}s")
        self.logger.info(f"Final processing rate: {final_rate:.1f} contracts/sec")
        
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
            
    
    def save_checkpoint(self, batch_num: int, total_batches: int, batch_size: int, offset: int):
        """Save processing checkpoint to enable resuming."""
        checkpoint = {
            'batch_num': batch_num,
            'total_batches': total_batches,
            'batch_size': batch_size,
            'offset': offset,
            'timestamp': time.time()
        }
        
        checkpoint_file = self.state_dir / "checkpoint.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    
    def load_checkpoint(self) -> Optional[Dict]:
        """Load processing checkpoint if it exists."""
        checkpoint_file = self.state_dir / "checkpoint.json"
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    # Check if checkpoint is recent (within 24 hours)
                    if time.time() - checkpoint['timestamp'] < 24 * 3600:
                        return checkpoint
                    else:
                        self.logger.info("Checkpoint is older than 24 hours, starting fresh")
            except Exception as e:
                self.logger.warning(f"Failed to load checkpoint: {e}")
        return None
    
    def clear_checkpoint(self):
        """Clear the checkpoint file after successful completion."""
        checkpoint_file = self.state_dir / "checkpoint.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
    
    def export_failed_contracts(self, output_file: Optional[str] = None) -> str:
        """Export failed contract submissions to a CSV file for analysis."""
        if output_file is None:
            output_file = self.state_dir / "failed_contracts.csv"
        
        with sqlite3.connect(self.db_path) as conn:
            failed_df = pd.read_sql_query("""
                SELECT address, chain_id, timestamp, error_message, tags_json
                FROM submissions 
                WHERE status = 'failed'
                ORDER BY timestamp DESC
            """, conn)
        
        if len(failed_df) > 0:
            failed_df.to_csv(output_file, index=False)
            self.logger.info(f"Exported {len(failed_df)} failed contracts to {output_file}")
        else:
            self.logger.info("No failed contracts to export")
            
        return str(output_file)


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