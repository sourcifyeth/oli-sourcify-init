#!/usr/bin/env python3
"""
Local Sourcify Data Processor using DuckDB

Processes locally downloaded Sourcify parquet files to extract OLI tags for verified contracts.
Optimized for fast local processing with efficient joins and minimal memory usage.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Iterator
import duckdb
import pandas as pd


def check_virtual_environment():
    """Check if running in a virtual environment and warn if not."""
    in_venv = (
        hasattr(sys, 'real_prefix') or  # virtualenv
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) or  # venv
        os.environ.get('VIRTUAL_ENV') is not None  # env variable
    )
    
    if not in_venv:
        print("⚠️  WARNING: Not running in a virtual environment!")
        print("   Setup: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt")
        print()
        
        try:
            response = input("Continue anyway? [y/N]: ").lower().strip()
            if response not in ['y', 'yes']:
                print("Exiting.")
                sys.exit(1)
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(1)


class LocalSourcifyProcessor:
    """Process locally downloaded Sourcify parquet files with DuckDB."""
    
    def __init__(self, data_dir: str = "./sourcify_data"):
        """
        Initialize the processor.
        
        Args:
            data_dir: Directory containing downloaded parquet files
        """
        self.data_dir = Path(data_dir)
        self.logger = self._setup_logger()
        self.conn = duckdb.connect()
        
        # Verify data directory exists
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
            
        self.logger.info(f"Initialized processor with data directory: {self.data_dir}")
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('LocalSourcifyProcessor')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
        
    def verify_data_files(self) -> Dict[str, List[Path]]:
        """
        Verify that required data files are present.
        
        Returns:
            Dictionary mapping table names to lists of parquet files
        """
        required_tables = ['verified_contracts', 'contract_deployments', 'compiled_contracts']
        file_map = {}
        
        for table_name in required_tables:
            table_dir = self.data_dir / table_name
            
            if not table_dir.exists():
                raise FileNotFoundError(f"Table directory not found: {table_dir}")
                
            parquet_files = list(table_dir.glob('*.parquet'))
            
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in: {table_dir}")
                
            file_map[table_name] = sorted(parquet_files)
            self.logger.info(f"{table_name}: {len(parquet_files)} files")
            
        return file_map
        
    def setup_duckdb_views(self, file_map: Dict[str, List[Path]]):
        """
        Create DuckDB views pointing to local parquet files.
        
        Args:
            file_map: Dictionary mapping table names to file paths
        """
        for table_name, file_paths in file_map.items():
            # Create file list for DuckDB
            file_patterns = [str(f) for f in file_paths]
            file_list = "['" + "', '".join(file_patterns) + "']"
            
            # Create view
            view_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet({file_list})"
            
            try:
                self.conn.execute(view_sql)
                self.logger.info(f"✓ Created view for {table_name}")
            except Exception as e:
                self.logger.error(f"Failed to create view for {table_name}: {e}")
                raise
                
    def get_data_statistics(self) -> Dict[str, int]:
        """Get basic statistics about the loaded data."""
        stats = {}
        
        tables = ['verified_contracts', 'contract_deployments', 'compiled_contracts']
        
        for table in tables:
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[table] = count
                self.logger.info(f"{table}: {count:,} records")
            except Exception as e:
                self.logger.error(f"Failed to count {table}: {e}")
                stats[table] = 0
                
        return stats
        
    def test_joins(self) -> Dict[str, int]:
        """Test join operations and return success statistics."""
        join_stats = {}
        
        # Test verified_contracts → contract_deployments join
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) 
                FROM verified_contracts vc
                JOIN contract_deployments cd ON vc.deployment_id = cd.id
                WHERE cd.chain_id IS NOT NULL
            """).fetchone()[0]
            join_stats['vc_to_deployments'] = result
            
        except Exception as e:
            self.logger.error(f"Join test vc→deployments failed: {e}")
            join_stats['vc_to_deployments'] = 0
            
        # Test verified_contracts → compiled_contracts join
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) 
                FROM verified_contracts vc
                JOIN compiled_contracts cc ON vc.compilation_id = cc.id
                WHERE cc.language IS NOT NULL
            """).fetchone()[0]
            join_stats['vc_to_compiled'] = result
            
        except Exception as e:
            self.logger.error(f"Join test vc→compiled failed: {e}")
            join_stats['vc_to_compiled'] = 0
            
        # Test full triple join
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) 
                FROM verified_contracts vc
                JOIN contract_deployments cd ON vc.deployment_id = cd.id
                JOIN compiled_contracts cc ON vc.compilation_id = cc.id
                WHERE cd.chain_id IS NOT NULL 
                  AND cd.address IS NOT NULL
                  AND cc.language IS NOT NULL
            """).fetchone()[0]
            join_stats['full_join'] = result
            
        except Exception as e:
            self.logger.error(f"Full join test failed: {e}")
            join_stats['full_join'] = 0
            
        return join_stats
        
    def get_chain_distribution(self, limit: int = 20) -> pd.DataFrame:
        """Get distribution of contracts across chains."""
        query = """
        SELECT 
            cd.chain_id,
            COUNT(*) as contract_count
        FROM verified_contracts vc
        JOIN contract_deployments cd ON vc.deployment_id = cd.id
        WHERE cd.chain_id IS NOT NULL
        GROUP BY cd.chain_id
        ORDER BY contract_count DESC
        LIMIT ?
        """
        
        try:
            result = self.conn.execute(query, [limit]).df()
            return result
        except Exception as e:
            self.logger.error(f"Failed to get chain distribution: {e}")
            return pd.DataFrame()
            
    def process_oli_batch(self, batch_size: int = 100000, offset: int = 0) -> pd.DataFrame:
        """
        Process a batch of contracts and return OLI-ready data.
        
        Args:
            batch_size: Number of contracts to process
            offset: Starting offset
            
        Returns:
            DataFrame with OLI tag data
        """
        query = """
        SELECT 
            vc.id as verified_contract_id,
            
            -- OLI tags
            'sourcify' as source_code_verified,
            true as is_contract,
            
            -- Contract deployment info
            cd.chain_id,
            '0x' || hex(cd.address) as address,
            CASE 
                WHEN cd.transaction_hash IS NOT NULL 
                THEN '0x' || hex(cd.transaction_hash)
                ELSE NULL 
            END as deployment_tx,
            cd.block_number as deployment_block,
            CASE 
                WHEN cd.deployer IS NOT NULL 
                THEN '0x' || hex(cd.deployer)
                ELSE NULL 
            END as deployer_address,
            
            -- Compilation info
            LOWER(cc.language) as code_language,
            cc.compiler || '-' || cc.version as code_compiler,
            
            -- Additional metadata
            vc.created_at as verified_at,
            cc.name as contract_name
            
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
            start_time = time.time()
            result = self.conn.execute(query, [batch_size, offset]).df()
            duration = time.time() - start_time
            
            self.logger.info(f"Processed batch: {len(result)} contracts in {duration:.1f}s (offset: {offset:,})")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process batch at offset {offset}: {e}")
            raise
            
    def process_all_contracts(self, batch_size: int = 100000, start_offset: int = 0) -> Iterator[pd.DataFrame]:
        """
        Iterator to process all contracts in batches.
        
        Args:
            batch_size: Size of each batch
            start_offset: Starting offset to resume from (useful for resuming processing)
            
        Yields:
            DataFrames containing contract batches ready for OLI
        """
        # Get total count first
        try:
            total_result = self.conn.execute("""
                SELECT COUNT(*) 
                FROM verified_contracts vc
                JOIN contract_deployments cd ON vc.deployment_id = cd.id
                JOIN compiled_contracts cc ON vc.compilation_id = cc.id
                WHERE cd.chain_id IS NOT NULL 
                  AND cd.address IS NOT NULL
                  AND cc.language IS NOT NULL
                  AND cd.address != '\\x0000000000000000000000000000000000000000'
            """).fetchone()[0]
            
            self.logger.info(f"Total contracts to process: {total_result:,}")
            
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            total_result = 0
            
        if total_result == 0:
            self.logger.warning("No contracts found to process")
            return
            
        # Process in batches starting from the specified offset
        offset = start_offset
        batch_num = 0 if start_offset == 0 else start_offset // batch_size
        
        if start_offset > 0:
            self.logger.info(f"Resuming processing from offset {start_offset:,} (batch {batch_num + 1})")
        
        while offset < total_result:
            batch_num += 1
            
            self.logger.info(f"Processing batch {batch_num} "
                           f"({offset:,} to {min(offset + batch_size, total_result):,})")
            
            batch = self.process_oli_batch(batch_size, offset)
            
            if batch.empty:
                break
                
            yield batch
            offset += len(batch)
            
            # Rate limiting to be nice to the system
            time.sleep(0.1)
            
    def preview_oli_data(self, limit: int = 5) -> Dict:
        """Preview OLI data structure with sample contracts."""
        self.logger.info(f"Generating preview with {limit} contracts...")
        
        try:
            sample = self.process_oli_batch(batch_size=limit, offset=0)
            
            if sample.empty:
                return {'error': 'No contracts found'}
                
            preview = {
                'sample_count': len(sample),
                'contracts': []
            }
            
            for _, row in sample.head(limit).iterrows():
                oli_tags = {
                    'source_code_verified': row.get('source_code_verified'),
                    'is_contract': row.get('is_contract'), 
                    'code_language': row.get('code_language'),
                    'code_compiler': row.get('code_compiler'),
                    'deployment_block': int(row['deployment_block']) if pd.notna(row.get('deployment_block')) else None,
                    'deployment_tx': row.get('deployment_tx'),
                    'deployer_address': row.get('deployer_address'),
                    'contract_name': row.get('contract_name')
                }
                
                contract_info = {
                    'address': row.get('address'),
                    'chain_id': row.get('chain_id'),
                    'contract_name': row.get('contract_name'),
                    'verified_at': str(row.get('verified_at')),
                    'oli_tags': oli_tags
                }
                
                preview['contracts'].append(contract_info)
                
            return preview
            
        except Exception as e:
            self.logger.error(f"Failed to generate preview: {e}")
            return {'error': str(e)}
            
    def get_processing_stats(self) -> Dict:
        """Get statistics useful for planning OLI processing."""
        stats = {}
        
        try:
            # Data completeness analysis
            result = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_contracts,
                    COUNT(cd.transaction_hash) as with_tx_hash,
                    COUNT(cd.block_number) as with_block_number,
                    COUNT(cd.deployer) as with_deployer,
                    COUNT(cc.language) as with_language,
                    COUNT(CASE WHEN cc.language IS NOT NULL THEN cc.version END) as with_compiler_version
                FROM verified_contracts vc
                JOIN contract_deployments cd ON vc.deployment_id = cd.id
                JOIN compiled_contracts cc ON vc.compilation_id = cc.id
                WHERE cd.chain_id IS NOT NULL 
                  AND cd.address IS NOT NULL
            """).fetchone()
            
            if result:
                total = result[0]
                stats['data_completeness'] = {
                    'total_contracts': total,
                    'with_deployment_tx': {'count': result[1], 'percentage': (result[1] / total * 100) if total > 0 else 0},
                    'with_deployment_block': {'count': result[2], 'percentage': (result[2] / total * 100) if total > 0 else 0},
                    'with_deployer_address': {'count': result[3], 'percentage': (result[3] / total * 100) if total > 0 else 0},
                    'with_code_language': {'count': result[4], 'percentage': (result[4] / total * 100) if total > 0 else 0},
                    'with_code_compiler': {'count': result[5], 'percentage': (result[5] / total * 100) if total > 0 else 0}
                }
            
            # Language distribution
            lang_dist = self.conn.execute("""
                SELECT 
                    LOWER(cc.language) as language,
                    COUNT(*) as count
                FROM verified_contracts vc
                JOIN compiled_contracts cc ON vc.compilation_id = cc.id
                WHERE cc.language IS NOT NULL
                GROUP BY LOWER(cc.language)
                ORDER BY count DESC
            """).df()
            
            stats['language_distribution'] = lang_dist.to_dict('records') if not lang_dist.empty else []
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get processing stats: {e}")
            return {'error': str(e)}


def main():
    """Main function for testing the local processor."""
    check_virtual_environment()
    
    print("🚀 Local Sourcify Data Processor")
    print("=" * 50)
    
    try:
        # Initialize processor
        processor = LocalSourcifyProcessor()
        
        # Step 1: Verify data files
        print("\n1. Verifying data files...")
        file_map = processor.verify_data_files()
        
        # Step 2: Setup DuckDB views
        print("\n2. Setting up DuckDB views...")
        processor.setup_duckdb_views(file_map)
        
        # Step 3: Get basic statistics
        print("\n3. Data statistics:")
        stats = processor.get_data_statistics()
        for table, count in stats.items():
            print(f"   {table}: {count:,} records")
            
        # Step 4: Test joins
        print("\n4. Testing joins...")
        join_stats = processor.test_joins()
        print(f"   Verified → Deployments: {join_stats.get('vc_to_deployments', 0):,} matches")
        print(f"   Verified → Compiled: {join_stats.get('vc_to_compiled', 0):,} matches") 
        print(f"   Full join (all 3 tables): {join_stats.get('full_join', 0):,} contracts")
        
        if join_stats.get('full_join', 0) == 0:
            print("   ⚠️  No successful joins found - check data integrity")
            return
            
        # Step 5: Chain distribution
        print("\n5. Chain distribution (top 10):")
        chain_dist = processor.get_chain_distribution(10)
        if not chain_dist.empty:
            for _, row in chain_dist.iterrows():
                print(f"   Chain {row['chain_id']}: {row['contract_count']:,} contracts")
        else:
            print("   No chain data found")
            
        # Step 6: Processing statistics
        print("\n6. Processing readiness:")
        proc_stats = processor.get_processing_stats()
        if 'data_completeness' in proc_stats:
            dc = proc_stats['data_completeness']
            print(f"   Total processable contracts: {dc['total_contracts']:,}")
            print(f"   With deployment_tx: {dc['with_deployment_tx']['count']:,} ({dc['with_deployment_tx']['percentage']:.1f}%)")
            print(f"   With deployment_block: {dc['with_deployment_block']['count']:,} ({dc['with_deployment_block']['percentage']:.1f}%)")
            print(f"   With deployer_address: {dc['with_deployer_address']['count']:,} ({dc['with_deployer_address']['percentage']:.1f}%)")
            
        # Step 7: Preview OLI data
        print("\n7. OLI data preview:")
        preview = processor.preview_oli_data(100)
        if 'contracts' in preview:
            for i, contract in enumerate(preview['contracts'], 1):
                print(f"   Contract {i}:")
                print(f"     Address: {contract['address']} (Chain: {contract['chain_id']})")
                print(f"     Language: {contract['oli_tags']['code_language']}")
                print(f"     Compiler: {contract['oli_tags']['code_compiler']}")
                complete_tags = sum(1 for v in contract['oli_tags'].values() if v is not None)
                print(f"     Complete tags: {complete_tags}/8")
        else:
            print(f"   Error: {preview.get('error', 'Unknown error')}")
            
        print(f"\n✅ Local processing test completed successfully!")
        print(f"Ready to process {join_stats.get('full_join', 0):,} verified contracts for OLI submission.")
        
        print("\nNext steps:")
        print("1. Set OLI_PRIVATE_KEY environment variable")
        print("2. Run full processing with OLI submission")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()