#!/usr/bin/env python3
"""
Test script for local Sourcify data processing

Tests the complete pipeline from local parquet files to OLI submission.
Validates data integrity, performance, and OLI tag generation.
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

from local_data_processor import LocalSourcifyProcessor, check_virtual_environment
from oli_submitter import OLISubmitter


def test_data_availability(processor: LocalSourcifyProcessor) -> bool:
    """Test if required data files are available and accessible."""
    print("🔍 Testing data availability...")
    
    try:
        file_map = processor.verify_data_files()
        
        total_files = sum(len(files) for files in file_map.values())
        print(f"✓ Found {total_files} parquet files across {len(file_map)} tables")
        
        for table, files in file_map.items():
            print(f"   {table}: {len(files)} files")
            
        return True
        
    except Exception as e:
        print(f"❌ Data availability test failed: {e}")
        return False


def test_duckdb_setup(processor: LocalSourcifyProcessor) -> bool:
    """Test DuckDB view creation and basic queries."""
    print("\n🔍 Testing DuckDB setup...")
    
    try:
        # Verify data and setup views
        file_map = processor.verify_data_files()
        processor.setup_duckdb_views(file_map)
        
        # Test basic statistics
        stats = processor.get_data_statistics()
        
        required_tables = ['verified_contracts', 'contract_deployments', 'compiled_contracts']
        for table in required_tables:
            if table not in stats or stats[table] == 0:
                print(f"❌ No data found in {table}")
                return False
                
        print(f"✓ Successfully created views and loaded data")
        print(f"   Total verified contracts: {stats.get('verified_contracts', 0):,}")
        print(f"   Total deployments: {stats.get('contract_deployments', 0):,}")
        print(f"   Total compiled contracts: {stats.get('compiled_contracts', 0):,}")
        
        return True
        
    except Exception as e:
        print(f"❌ DuckDB setup test failed: {e}")
        return False


def test_joins_performance(processor: LocalSourcifyProcessor) -> bool:
    """Test join performance and data integrity."""
    print("\n🔍 Testing joins and performance...")
    
    try:
        start_time = time.time()
        join_stats = processor.test_joins()
        duration = time.time() - start_time
        
        print(f"✓ Join tests completed in {duration:.2f}s")
        
        # Check join results
        vc_deployments = join_stats.get('vc_to_deployments', 0)
        vc_compiled = join_stats.get('vc_to_compiled', 0)  
        full_join = join_stats.get('full_join', 0)
        
        print(f"   Verified → Deployments: {vc_deployments:,} matches")
        print(f"   Verified → Compiled: {vc_compiled:,} matches")
        print(f"   Full 3-way join: {full_join:,} contracts")
        
        # Validate reasonable join success rates
        if full_join == 0:
            print("❌ No successful 3-way joins found")
            return False
            
        if full_join < min(vc_deployments, vc_compiled) * 0.8:
            print(f"⚠️  Low join success rate - only {full_join:,} out of expected ~{min(vc_deployments, vc_compiled):,}")
            
        return True
        
    except Exception as e:
        print(f"❌ Join performance test failed: {e}")
        return False


def test_oli_data_generation(processor: LocalSourcifyProcessor) -> bool:
    """Test OLI tag generation and data quality."""
    print("\n🔍 Testing OLI data generation...")
    
    try:
        # Test small batch processing
        test_batch_size = 1000
        start_time = time.time()
        
        batch = processor.process_oli_batch(batch_size=test_batch_size, offset=0)
        duration = time.time() - start_time
        
        if batch.empty:
            print("❌ No contracts returned from batch processing")
            return False
            
        print(f"✓ Processed {len(batch)} contracts in {duration:.2f}s ({len(batch)/duration:.0f} contracts/sec)")
        
        # Check required columns
        required_oli_columns = [
            'address', 'chain_id', 'source_code_verified', 'is_contract',
            'code_language', 'code_compiler', 'deployment_block', 
            'deployment_tx', 'deployer_address'
        ]
        
        missing_columns = [col for col in required_oli_columns if col not in batch.columns]
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return False
            
        print(f"✓ All required OLI columns present")
        
        # Data quality checks
        null_address = batch['address'].isnull().sum()
        null_chain = batch['chain_id'].isnull().sum() 
        null_language = batch['code_language'].isnull().sum()
        
        print(f"   Data quality:")
        print(f"     Addresses: {len(batch) - null_address}/{len(batch)} ({(1-null_address/len(batch))*100:.1f}%)")
        print(f"     Chain IDs: {len(batch) - null_chain}/{len(batch)} ({(1-null_chain/len(batch))*100:.1f}%)")
        print(f"     Languages: {len(batch) - null_language}/{len(batch)} ({(1-null_language/len(batch))*100:.1f}%)")
        
        # Check for valid addresses (should start with 0x and be 42 chars)
        valid_addresses = batch['address'].str.match(r'^0x[0-9a-fA-F]{40}$').sum()
        print(f"     Valid addresses: {valid_addresses}/{len(batch)} ({valid_addresses/len(batch)*100:.1f}%)")
        
        if valid_addresses < len(batch) * 0.95:
            print("⚠️  Low percentage of valid addresses")
            
        return True
        
    except Exception as e:
        print(f"❌ OLI data generation test failed: {e}")
        return False


def test_batch_iteration(processor: LocalSourcifyProcessor) -> bool:
    """Test batch iteration functionality."""
    print("\n🔍 Testing batch iteration...")
    
    try:
        test_batch_size = 5000
        max_batches = 3  # Test first 3 batches only
        
        batch_count = 0
        total_contracts = 0
        start_time = time.time()
        
        for batch in processor.process_all_contracts(batch_size=test_batch_size):
            batch_count += 1
            total_contracts += len(batch)
            
            print(f"   Batch {batch_count}: {len(batch)} contracts")
            
            # Basic validation
            if batch.empty:
                print(f"❌ Empty batch {batch_count}")
                return False
                
            # Check for duplicate addresses within batch
            duplicates = batch['address'].duplicated().sum()
            if duplicates > 0:
                print(f"⚠️  Found {duplicates} duplicate addresses in batch {batch_count}")
                
            if batch_count >= max_batches:
                break
                
        duration = time.time() - start_time
        
        if batch_count == 0:
            print("❌ No batches processed")
            return False
            
        print(f"✓ Processed {batch_count} batches ({total_contracts} contracts) in {duration:.2f}s")
        print(f"   Average: {total_contracts/duration:.0f} contracts/sec")
        
        return True
        
    except Exception as e:
        print(f"❌ Batch iteration test failed: {e}")
        return False


def test_oli_submitter_integration(processor: LocalSourcifyProcessor) -> bool:
    """Test integration with OLI submitter (without actual submission)."""
    print("\n🔍 Testing OLI submitter integration...")
    
    try:
        # Get a small batch for testing
        test_batch = processor.process_oli_batch(batch_size=100, offset=0)
        
        if test_batch.empty:
            print("❌ No test data available")
            return False
            
        # Test OLI tag formatting (without submitting)
        dummy_key = "0x" + "1" * 64  # Valid format but dummy key
        oli_submitter = OLISubmitter(private_key=dummy_key, is_production=False)
        
        # Convert first contract to OLI format
        first_contract = test_batch.iloc[0]
        
        oli_tags = {
            'source_code_verified': first_contract.get('source_code_verified'),
            'is_contract': bool(first_contract.get('is_contract')),
            'code_language': first_contract.get('code_language'),
            'code_compiler': first_contract.get('code_compiler'),
            'deployment_block': int(first_contract['deployment_block']) if first_contract.get('deployment_block') else None,
            'deployment_tx': first_contract.get('deployment_tx'),
            'deployer_address': first_contract.get('deployer_address')
        }
        
        # Validate tag format  
        oli_chain_id = f"eip155:{first_contract.get('chain_id')}"
        valid_tags = oli_submitter.validate_submission(first_contract.get('address'), oli_chain_id, oli_tags)
        
        print(f"✓ OLI tag validation successful")
        print(f"   Valid tags: {sum(1 for v in oli_tags.values() if v is not None)}/7")
        print(f"   Sample contract: {first_contract.get('address')} (Chain: {first_contract.get('chain_id')})")
        
        return True
        
    except Exception as e:
        print(f"❌ OLI submitter integration test failed: {e}")
        return False


def test_performance_benchmarks(processor: LocalSourcifyProcessor) -> Dict:
    """Run performance benchmarks and return metrics."""
    print("\n🔍 Running performance benchmarks...")
    
    benchmarks = {}
    
    try:
        # Test different batch sizes
        batch_sizes = [1000, 5000, 10000]
        
        for batch_size in batch_sizes:
            start_time = time.time()
            batch = processor.process_oli_batch(batch_size=batch_size, offset=0)
            duration = time.time() - start_time
            
            if not batch.empty:
                rate = len(batch) / duration
                benchmarks[f'batch_{batch_size}'] = {
                    'contracts': len(batch),
                    'duration_sec': round(duration, 2),
                    'contracts_per_sec': round(rate, 0)
                }
                print(f"   {batch_size} contracts: {duration:.2f}s ({rate:.0f} contracts/sec)")
            else:
                print(f"   ⚠️  Empty result for batch size {batch_size}")
                
        # Estimate full processing time
        if benchmarks:
            # Use largest successful batch for estimation
            best_rate = max(b['contracts_per_sec'] for b in benchmarks.values() if b['contracts_per_sec'] > 0)
            
            # Get estimated total contracts
            stats = processor.get_data_statistics()
            total_verified = stats.get('verified_contracts', 0)
            
            if total_verified > 0 and best_rate > 0:
                estimated_time = total_verified / best_rate
                benchmarks['estimated_full_processing'] = {
                    'total_contracts': total_verified,
                    'estimated_hours': round(estimated_time / 3600, 1),
                    'best_rate_per_sec': best_rate
                }
                print(f"   Estimated full processing: {estimated_time/3600:.1f} hours at {best_rate:.0f} contracts/sec")
                
        return benchmarks
        
    except Exception as e:
        print(f"❌ Performance benchmark failed: {e}")
        return {}


def run_comprehensive_test() -> bool:
    """Run all tests and return overall success."""
    print("🚀 Comprehensive Local Processing Test")
    print("=" * 60)
    
    # Check environment
    check_virtual_environment()
    
    try:
        # Initialize processor
        processor = LocalSourcifyProcessor()
        
        # Run test suite
        tests = [
            ("Data Availability", lambda: test_data_availability(processor)),
            ("DuckDB Setup", lambda: test_duckdb_setup(processor)),
            ("Join Performance", lambda: test_joins_performance(processor)),
            ("OLI Data Generation", lambda: test_oli_data_generation(processor)), 
            ("Batch Iteration", lambda: test_batch_iteration(processor)),
            ("OLI Integration", lambda: test_oli_submitter_integration(processor)),
        ]
        
        results = {}
        
        for test_name, test_func in tests:
            print(f"\n{'='*20} {test_name} {'='*20}")
            try:
                results[test_name] = test_func()
            except Exception as e:
                print(f"❌ {test_name} failed with exception: {e}")
                results[test_name] = False
                
        # Performance benchmarks (non-blocking)
        print(f"\n{'='*20} Performance Benchmarks {'='*20}")
        benchmarks = test_performance_benchmarks(processor)
        
        # Summary
        print(f"\n{'='*60}")
        print("🏁 TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for success in results.values() if success)
        total = len(results)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"   {test_name:<25} {status}")
            
        print(f"\nOverall: {passed}/{total} tests passed")
        
        if benchmarks:
            print(f"\nPerformance Summary:")
            for key, metrics in benchmarks.items():
                if key.startswith('batch_'):
                    print(f"   {key}: {metrics['contracts_per_sec']} contracts/sec")
                elif key == 'estimated_full_processing':
                    print(f"   Full processing estimate: {metrics['estimated_hours']} hours")
                    
        # Recommendations
        if passed == total:
            print(f"\n🎉 All tests passed! System is ready for production OLI processing.")
        else:
            print(f"\n⚠️  {total - passed} test(s) failed. Review errors before production use.")
            
        return passed == total
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        return False


def main():
    """Main test entry point."""
    
    # Check if data directory exists
    data_dir = Path("./sourcify_data")
    if not data_dir.exists():
        print("❌ Data directory './sourcify_data' not found!")
        print("   Run 'python download_parquet_files.py' first to download data.")
        sys.exit(1)
        
    success = run_comprehensive_test()
    
    if success:
        print(f"\n✅ Ready for production! Next steps:")
        print("1. Set OLI_PRIVATE_KEY environment variable")
        print("2. Run: python main.py")
        sys.exit(0)
    else:
        print(f"\n❌ Tests failed. Fix issues before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()