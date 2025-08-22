#!/usr/bin/env python3
"""
Test the data processing pipeline
"""

import os
import sys
import pandas as pd
from sourcify_data_processor import SourcifyDataProcessor, check_virtual_environment


def main():
    """Test the data processing functionality."""
    check_virtual_environment()
    
    print("Testing Sourcify Data Processing with DuckDB...")
    
    processor = SourcifyDataProcessor()
    
    try:
        # Step 1: Load manifest and setup
        print("\n1. Loading manifest...")
        manifest = processor.get_manifest()
        print(f"   ✓ Manifest loaded successfully")
        
        # Step 2: Create DuckDB views
        print("\n2. Setting up DuckDB views...")
        processor.create_table_views(manifest)
        print(f"   ✓ Views created successfully")
        
        # Step 3: Get basic statistics
        print("\n3. Getting contract statistics...")
        total_contracts = processor.get_contract_count()
        print(f"   ✓ Total verified contracts: {total_contracts:,}")
        
        # Step 4: Debug data first
        print("\n4. Debug Information:")
        debug_info = processor.debug_chain_data()
        
        for key, value in debug_info.items():
            if key == 'all_chains_sample' and hasattr(value, 'shape'):
                print(f"   {key}: {value.shape[0]} total chains")
                print("   Top 10 chains:")
                # Add indentation manually
                chain_table = value.head(10).to_string(index=False)
                for line in chain_table.split('\n'):
                    print(f"     {line}")
            else:
                print(f"   {key}: {value}")
        
        # Step 5: Chain statistics from join
        print("\n5. Chain Statistics (from join):")
        chain_stats = processor.get_chain_statistics()
        print(f"   Showing {len(chain_stats)} out of {debug_info.get('all_chains_count', 'unknown')} total chains")
        # Add indentation manually
        stats_table = chain_stats.head(10).to_string(index=False)
        for line in stats_table.split('\n'):
            print(f"   {line}")
        
        # Step 6: Preview OLI data structure
        print("\n6. Preview OLI data structure:")
        processor.preview_oli_data(2)
        
        # Step 7: Process a test batch
        print("\n7. Processing test batch...")
        test_batch = processor.process_contracts_batch(batch_size=5000, offset=0)
        print(f"   ✓ Successfully processed {len(test_batch)} contracts")
        
        # Step 8: Show data quality
        print("\n8. Data completeness analysis:")
        completeness = {
            'total_contracts': len(test_batch),
            'with_deployment_tx': test_batch['deployment_tx'].notna().sum(),
            'with_deployment_block': test_batch['deployment_block'].notna().sum(), 
            'with_deployer_address': test_batch['deployer_address'].notna().sum(),
            'with_code_language': test_batch['code_language'].notna().sum(),
            'with_code_compiler': test_batch['code_compiler'].notna().sum()
        }
        
        for key, value in completeness.items():
            if key == 'total_contracts':
                print(f"   • {key}: {value:,}")
            else:
                percentage = (value / completeness['total_contracts']) * 100
                print(f"   • {key}: {value:,} ({percentage:.1f}%)")
                
        # Step 9: Show sample data
        print(f"\n9. Sample contract data:")
        sample = test_batch.head(3)
        for idx, row in sample.iterrows():
            print(f"\n   Contract {idx + 1}:")
            print(f"     Address: {row['address']} (Chain: {row['chain_id']})")
            print(f"     Language: {row['code_language']}")
            print(f"     Compiler: {row['code_compiler']}")
            print(f"     Deployment Block: {row['deployment_block']}")
            print(f"     Has TX: {'✓' if pd.notna(row['deployment_tx']) else '✗'}")
            print(f"     Has Deployer: {'✓' if pd.notna(row['deployer_address']) else '✗'}")
        
        print(f"\n✅ Data processing test completed successfully!")
        print(f"Ready to process {total_contracts:,} verified contracts for OLI submission.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    main()