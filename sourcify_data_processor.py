#!/usr/bin/env python3
"""
Sourcify Data Processor using DuckDB

Processes verified contracts from Sourcify parquet exports and prepares data for OLI submission.
Uses DuckDB for efficient out-of-core processing of large datasets.
"""

import json
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Iterator
import duckdb
import pandas as pd
import requests


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


class SourcifyDataProcessor:
    """Process Sourcify parquet exports to extract contract data for OLI tagging."""
    
    def __init__(self, base_url: str = "https://export.sourcify.dev"):
        """
        Initialize the data processor.
        
        Args:
            base_url: Base URL for Sourcify exports (default: https://export.sourcify.dev)
        """
        self.base_url = base_url
        self.logger = self._setup_logger()
        self.conn = duckdb.connect()
        
        # Configure DuckDB for public R2 bucket (no credentials needed)
        self.conn.execute("SET s3_region='auto'")
        self.conn.execute("SET s3_access_key_id=''")
        self.conn.execute("SET s3_secret_access_key=''")
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('SourcifyDataProcessor')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
        
    def get_manifest(self) -> Dict:
        """
        Download and parse the manifest file to get available parquet files.
        
        Returns:
            Dictionary containing file information
        """
        try:
            response = requests.get(f"{self.base_url}/manifest.json", timeout=30)
            response.raise_for_status()
            manifest = response.json()
            
            self.logger.info(f"Manifest loaded - timestamp: {manifest.get('timestamp')}")
            self.logger.info(f"Date: {manifest.get('dateStr')}")
            
            return manifest
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch manifest: {e}")
            raise
            
    def get_table_file_urls(self, manifest: Dict, table_name: str) -> List[str]:
        """
        Get URLs for all parquet files of a specific table.
        
        Args:
            manifest: Manifest dictionary
            table_name: Name of the table (e.g., 'verified_contracts')
            
        Returns:
            List of URLs for the table's parquet files
        """
        files = manifest.get('files', {}).get(table_name, [])
        urls = [f"{self.base_url}/{file_path}" for file_path in files]
        
        self.logger.info(f"Found {len(urls)} files for table '{table_name}'")
        return urls
        
    def create_table_views(self, manifest: Dict):
        """
        Create DuckDB views for each table pointing to the parquet files.
        
        Args:
            manifest: Manifest dictionary with file information
        """
        tables = ['verified_contracts', 'contract_deployments', 'compiled_contracts']
        
        for table_name in tables:
            files = manifest.get('files', {}).get(table_name, [])
            if not files:
                self.logger.warning(f"No files found for table '{table_name}'")
                continue
                
            # Create list of full URLs for each file
            file_urls = [f"'{self.base_url}/{file_path}'" for file_path in files]
            file_list = '[' + ', '.join(file_urls) + ']'
            
            # Create view using list of specific files
            view_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet({file_list})"
            
            try:
                self.conn.execute(view_sql)
                self.logger.info(f"Created view for table '{table_name}' with {len(files)} files")
            except Exception as e:
                self.logger.error(f"Failed to create view for '{table_name}': {e}")
                raise
                
    def get_contract_count(self) -> int:
        """
        Get total count of verified contracts.
        
        Returns:
            Total number of verified contracts
        """
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM verified_contracts").fetchone()
            count = result[0] if result else 0
            self.logger.info(f"Total verified contracts: {count:,}")
            return count
        except Exception as e:
            self.logger.error(f"Failed to get contract count: {e}")
            raise
            
    def get_sample_contracts(self, limit: int = 10) -> pd.DataFrame:
        """
        Get a sample of verified contracts with all joined data.
        
        Args:
            limit: Number of contracts to retrieve
            
        Returns:
            DataFrame with sample contract data including all OLI tag fields
        """
        query = """
        SELECT 
            vc.id as verified_contract_id,
            vc.created_at as verified_at,
            
            -- Contract deployment info  
            cd.chain_id,
            '0x' || encode(cd.address, 'hex') as address,
            CASE 
                WHEN cd.transaction_hash IS NOT NULL 
                THEN '0x' || encode(cd.transaction_hash, 'hex')
                ELSE NULL 
            END as deployment_tx,
            cd.block_number as deployment_block,
            CASE 
                WHEN cd.deployer IS NOT NULL 
                THEN '0x' || encode(cd.deployer, 'hex')
                ELSE NULL 
            END as deployer_address,
            
            -- Compilation info
            cc.language as code_language,
            cc.compiler,
            cc.version,
            cc.compiler || '-' || cc.version as code_compiler,
            cc.name as contract_name,
            cc.fully_qualified_name,
            
            -- Verification status
            vc.creation_match,
            vc.runtime_match,
            
            -- Additional info
            vc.creation_metadata_match,
            vc.runtime_metadata_match
            
        FROM verified_contracts vc
        JOIN contract_deployments cd ON vc.deployment_id = cd.id
        JOIN compiled_contracts cc ON vc.compilation_id = cc.id
        
        WHERE cd.chain_id IS NOT NULL 
          AND cd.address IS NOT NULL
          AND cc.language IS NOT NULL
          
        ORDER BY vc.created_at DESC
        LIMIT ?
        """
        
        try:
            result = self.conn.execute(query, [limit]).df()
            self.logger.info(f"Retrieved {len(result)} sample contracts")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get sample contracts: {e}")
            raise
            
    def process_contracts_batch(self, batch_size: int = 100000, offset: int = 0) -> pd.DataFrame:
        """
        Process a batch of verified contracts with all required OLI data.
        
        Args:
            batch_size: Number of contracts to process in this batch
            offset: Offset to start from (for pagination)
            
        Returns:
            DataFrame with contract data ready for OLI processing
        """
        query = """
        SELECT 
            vc.id as verified_contract_id,
            
            -- Required OLI tags
            'sourcify' as source_code_verified,
            true as is_contract,
            
            -- Contract deployment info  
            cd.chain_id,
            '0x' || encode(cd.address, 'hex') as address,
            CASE 
                WHEN cd.transaction_hash IS NOT NULL 
                THEN '0x' || encode(cd.transaction_hash, 'hex')
                ELSE NULL 
            END as deployment_tx,
            cd.block_number as deployment_block,
            CASE 
                WHEN cd.deployer IS NOT NULL 
                THEN '0x' || encode(cd.deployer, 'hex')
                ELSE NULL 
            END as deployer_address,
            
            -- Compilation info
            LOWER(cc.language) as code_language,
            cc.compiler || '-' || cc.version as code_compiler
            
        FROM verified_contracts vc
        JOIN contract_deployments cd ON vc.deployment_id = cd.id
        JOIN compiled_contracts cc ON vc.compilation_id = cc.id
        
        WHERE cd.chain_id IS NOT NULL 
          AND cd.address IS NOT NULL
          AND cc.language IS NOT NULL
          AND cd.address != '\\x0000000000000000000000000000000000000000'
          
        ORDER BY vc.id
        LIMIT ? OFFSET ?
        """
        
        try:
            result = self.conn.execute(query, [batch_size, offset]).df()
            self.logger.info(f"Processed batch: {len(result)} contracts (offset: {offset:,})")
            return result
        except Exception as e:
            self.logger.error(f"Failed to process batch at offset {offset}: {e}")
            raise
            
    def get_chain_statistics(self) -> pd.DataFrame:
        """
        Get statistics about contracts per chain.
        
        Returns:
            DataFrame with chain statistics
        """
        query = """
        SELECT 
            cd.chain_id,
            COUNT(*) as contract_count,
            COUNT(CASE WHEN cd.transaction_hash IS NOT NULL THEN 1 END) as contracts_with_tx,
            COUNT(CASE WHEN cd.deployer IS NOT NULL THEN 1 END) as contracts_with_deployer,
            MIN(vc.created_at) as first_verified,
            MAX(vc.created_at) as last_verified
        FROM verified_contracts vc
        JOIN contract_deployments cd ON vc.deployment_id = cd.id
        WHERE cd.chain_id IS NOT NULL
        GROUP BY cd.chain_id
        ORDER BY contract_count DESC
        """
        
        try:
            result = self.conn.execute(query).df()
            self.logger.info(f"Retrieved statistics for {len(result)} chains")
            return result
        except Exception as e:
            self.logger.error(f"Failed to get chain statistics: {e}")
            raise
            
    def process_all_contracts(self, batch_size: int = 100000) -> Iterator[pd.DataFrame]:
        """
        Iterator to process all verified contracts in batches.
        
        Args:
            batch_size: Number of contracts to process per batch
            
        Yields:
            DataFrames containing batches of contract data
        """
        total_contracts = self.get_contract_count()
        offset = 0
        
        while offset < total_contracts:
            self.logger.info(f"Processing batch {offset//batch_size + 1} "
                           f"({offset:,} to {min(offset + batch_size, total_contracts):,})")
            
            batch = self.process_contracts_batch(batch_size, offset)
            
            if batch.empty:
                break
                
            yield batch
            offset += batch_size
            
            # Small delay to be nice to the servers
            time.sleep(0.1)
            
    def preview_oli_data(self, limit: int = 5):
        """
        Preview what the OLI data will look like for a few contracts.
        
        Args:
            limit: Number of contracts to preview
        """
        sample = self.get_sample_contracts(limit)
        
        print(f"\n=== Preview of {len(sample)} Verified Contracts for OLI ===\n")
        
        for idx, row in sample.iterrows():
            oli_tags = {
                'source_code_verified': 'sourcify',
                'is_contract': True,
                'code_language': row['code_language'].lower() if pd.notna(row['code_language']) else None,
                'code_compiler': row['code_compiler'] if pd.notna(row['code_compiler']) else None,
                'deployment_block': int(row['deployment_block']) if pd.notna(row['deployment_block']) else None,
                'deployment_tx': row['deployment_tx'] if pd.notna(row['deployment_tx']) else None,
                'deployer_address': row['deployer_address'] if pd.notna(row['deployer_address']) else None,
            }
            
            print(f"Contract {idx + 1}:")
            print(f"  Address: {row['address']} (Chain: {row['chain_id']})")
            print(f"  Contract: {row['contract_name']} ({row['fully_qualified_name']})")
            print(f"  Verified: {row['verified_at']}")
            print(f"  OLI Tags: {json.dumps(oli_tags, indent=4, default=str)}")
            print("-" * 80)


def main():
    """Example usage of SourcifyDataProcessor."""
    check_virtual_environment()
    processor = SourcifyDataProcessor()
    
    try:
        # Get manifest and setup views
        print("Loading manifest and setting up DuckDB views...")
        manifest = processor.get_manifest()
        processor.create_table_views(manifest)
        
        # Show statistics
        print("\n=== Chain Statistics ===")
        chain_stats = processor.get_chain_statistics()
        print(chain_stats.head(10).to_string(index=False))
        
        # Preview OLI data
        processor.preview_oli_data(3)
        
        # Process a small batch to test
        print("\n=== Processing Test Batch ===")
        test_batch = processor.process_contracts_batch(batch_size=1000, offset=0)
        print(f"Successfully processed {len(test_batch)} contracts")
        print(f"Sample columns: {list(test_batch.columns)}")
        
        # Show data completeness
        print(f"\nData Completeness:")
        print(f"- Contracts with deployment_tx: {test_batch['deployment_tx'].notna().sum()}")
        print(f"- Contracts with deployment_block: {test_batch['deployment_block'].notna().sum()}")
        print(f"- Contracts with deployer_address: {test_batch['deployer_address'].notna().sum()}")
        print(f"- Contracts with code_language: {test_batch['code_language'].notna().sum()}")
        print(f"- Contracts with code_compiler: {test_batch['code_compiler'].notna().sum()}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()