#!/usr/bin/env python3
"""
Main entry point for Sourcify OLI Bridge

Processes all verified contracts from locally downloaded Sourcify data and submits them to OLI.
"""

import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

from local_data_processor import LocalSourcifyProcessor, check_virtual_environment
from oli_submitter import OLISubmitter


def get_config() -> Dict[str, Any]:
    """Get configuration from environment variables."""
    # Load environment variables from .env file
    load_dotenv()
    
    config = {
        'oli_private_key': os.getenv('OLI_PRIVATE_KEY'),
        'use_production': os.getenv('USE_PRODUCTION', 'false').lower() == 'true',
        'batch_size': int(os.getenv('DEFAULT_BATCH_SIZE', '1000')),
        'submission_delay': float(os.getenv('SUBMISSION_DELAY', '1.0')),
        'submit_onchain': os.getenv('SUBMIT_ONCHAIN', 'false').lower() == 'true',
        'data_dir': os.getenv('DATA_DIR', './sourcify_data'),
    }
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration and check prerequisites."""
    
    # Check OLI private key
    if not config['oli_private_key']:
        print("❌ OLI_PRIVATE_KEY environment variable not set!")
        print("   Set it with: export OLI_PRIVATE_KEY='your_private_key_here'")
        return False
    
    # Check data directory exists
    data_dir = Path(config['data_dir'])
    if not data_dir.exists():
        print(f"❌ Data directory '{data_dir}' not found!")
        print("   Run 'python download_parquet_files.py' first to download data.")
        return False
        
    # Check for required data subdirectories
    required_dirs = ['verified_contracts', 'contract_deployments', 'compiled_contracts']
    for dir_name in required_dirs:
        subdir = data_dir / dir_name
        if not subdir.exists():
            print(f"❌ Required data directory '{subdir}' not found!")
            print("   Run 'python download_parquet_files.py' to download all required data.")
            return False
            
    return True


def print_config_summary(config: Dict[str, Any]):
    """Print configuration summary."""
    print("📋 Configuration:")
    print(f"   Network: {'Base Mainnet (PRODUCTION)' if config['use_production'] else 'Base Sepolia (TESTNET)'}")
    print(f"   Batch size: {config['batch_size']:,} contracts")
    print(f"   Submission delay: {config['submission_delay']}s")
    print(f"   Submission type: {'Onchain (costs gas)' if config['submit_onchain'] else 'Offchain (free)'}")
    if not config['submit_onchain']:
        max_workers = int(os.getenv('MAX_PARALLEL_WORKERS', '10'))
        print(f"   Parallel workers: {max_workers} (offchain only)")
    print(f"   Data directory: {config['data_dir']}")


def main():
    """Main processing function."""
    print("🚀 Sourcify OLI Bridge - Main Processor")
    print("=" * 60)
    
    # Check virtual environment
    check_virtual_environment()
    
    # Get and validate configuration
    config = get_config()
    if not validate_config(config):
        sys.exit(1)
        
    print_config_summary(config)
    
    # Warn about production mode
    if config['use_production']:
        print("\n⚠️  PRODUCTION MODE ENABLED - This will submit to Base Mainnet!")
        try:
            response = input("Are you sure you want to continue? [y/N]: ").lower().strip()
            if response not in ['y', 'yes']:
                print("Cancelled.")
                sys.exit(0)
        except (KeyboardInterrupt, EOFError):
            print("\nCancelled.")
            sys.exit(0)
    
    # Warn about onchain submissions
    if config['submit_onchain']:
        print("\n⚠️  ONCHAIN SUBMISSIONS ENABLED - This will cost gas!")
        print("   Make sure your wallet has sufficient ETH on Base.")
        try:
            response = input("Continue with onchain submissions? [y/N]: ").lower().strip()
            if response not in ['y', 'yes']:
                print("Cancelled.")
                sys.exit(0)
        except (KeyboardInterrupt, EOFError):
            print("\nCancelled.")
            sys.exit(0)
    
    print(f"\n🔄 Initializing processors...")
    
    # Initialize tracking variables outside try block
    total_successful = 0
    total_processed = 0
    
    try:
        # Initialize processor and submitter with state tracking
        processor = LocalSourcifyProcessor(data_dir=config['data_dir'])
        submitter = OLISubmitter(
            private_key=config['oli_private_key'], 
            is_production=config['use_production'],
            state_dir=config.get('state_dir', './oli_state')
        )
        
        # Quick data validation
        print("\n1. Validating data...")
        file_map = processor.verify_data_files()
        processor.setup_duckdb_views(file_map)
        
        stats = processor.get_data_statistics()
        join_stats = processor.test_joins()
        
        total_processable = join_stats.get('full_join', 0)
        
        if total_processable == 0:
            print("❌ No processable contracts found in data!")
            print("   Check data integrity with: python test_local_processing.py")
            sys.exit(1)
            
        print(f"✅ Found {total_processable:,} processable contracts")
        
        # Check existing submission state
        state_stats = submitter.get_submission_stats()
        if state_stats['total_submissions'] > 0:
            print(f"\n📊 Previous submission state:")
            print(f"   Total attempts: {state_stats['total_submissions']:,}")
            print(f"   Successful: {state_stats['successful']:,} ({state_stats['success_rate']:.1f}%)")
            print(f"   Failed: {state_stats['failed']:,}")
            print(f"   Remaining to process: {total_processable - state_stats['successful']:,}")
        else:
            print(f"\n📊 Fresh start - no previous submissions found")
        
        # Check for existing checkpoint
        checkpoint = submitter.load_checkpoint()
        remaining_contracts = total_processable - state_stats.get('successful', 0)
        
        # Calculate resume offset from checkpoint
        resume_offset = 0
        if checkpoint:
            # Resume from the checkpoint offset minus one batch to be safe
            checkpoint_offset = checkpoint.get('offset', 0)
            batch_size_from_checkpoint = checkpoint.get('batch_size', config['batch_size'])
            resume_offset = max(0, checkpoint_offset - batch_size_from_checkpoint)
            if resume_offset > 0:
                remaining_contracts = total_processable - resume_offset
                print(f"\n🔄 Will resume from offset {resume_offset:,} (one batch before checkpoint for safety)")
            else:
                remaining_contracts = total_processable
        
        # Estimate processing time for remaining contracts
        estimated_batches = (remaining_contracts + config['batch_size'] - 1) // config['batch_size']
        estimated_time_sec = estimated_batches * config['submission_delay']
        estimated_hours = estimated_time_sec / 3600
        
        print(f"\n📊 Processing estimate:")
        print(f"   Total contracts: {total_processable:,}")
        print(f"   Remaining to process: {remaining_contracts:,}")
        print(f"   Batch size: {config['batch_size']:,}")
        print(f"   Estimated batches: {estimated_batches:,}")
        print(f"   Estimated time: {estimated_hours:.1f} hours")
        print(f"   Network: {'Base Mainnet (PRODUCTION)' if config['use_production'] else 'Base Sepolia (TESTNET)'}")
        print(f"   Attestation address: {submitter.oli.address}")
        
        if checkpoint:
            print(f"\n🔄 Resumable checkpoint found:")
            print(f"   Previous batch: {checkpoint['batch_num']}/{checkpoint['total_batches']}")
            print(f"   Automatically resuming from where you left off...")
        else:
            # Only ask for confirmation if starting fresh
            print(f"\n🎯 Ready to process all contracts!")
            try:
                response = input("Start processing? [y/N]: ").lower().strip()
                if response not in ['y', 'yes']:
                    print("Processing cancelled.")
                    sys.exit(0)
            except (KeyboardInterrupt, EOFError):
                print("\nProcessing cancelled.")
                sys.exit(0)
        
        # Main processing loop
        print(f"\n🔄 Starting batch processing...")
        print("=" * 60)
        
        batch_num = resume_offset // config['batch_size'] if resume_offset > 0 else 0
        start_time = time.time()
        
        for batch in processor.process_all_contracts(batch_size=config['batch_size'], start_offset=resume_offset):
            # Check for shutdown signal before processing batch
            if submitter.shutdown_requested:
                print(f"\n🛑 Shutdown requested - stopping gracefully after batch {batch_num}")
                break
                
            batch_num += 1
            
            print(f"\n📦 Batch {batch_num} ({len(batch)} contracts)")
            
            # Save checkpoint before processing batch
            estimated_total_batches = (total_processable + config['batch_size'] - 1) // config['batch_size']
            current_offset = resume_offset + (batch_num - (resume_offset // config['batch_size'])) * config['batch_size']
            submitter.save_checkpoint(batch_num, estimated_total_batches, config['batch_size'], current_offset)
            
            # Submit batch to OLI
            batch_start = time.time()
            successful, total = submitter.submit_batch(
                batch, 
                submit_onchain=config['submit_onchain'],
                delay=config['submission_delay']
            )
            batch_duration = time.time() - batch_start
            
            total_successful += successful
            total_processed += total
            
            # Progress summary
            success_rate = (successful / total * 100) if total > 0 else 0
            overall_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0
            elapsed_time = time.time() - start_time
            
            print(f"   Batch result: {successful}/{total} successful ({success_rate:.1f}%)")
            print(f"   Overall: {total_successful:,}/{total_processed:,} successful ({overall_rate:.1f}%)")
            print(f"   Elapsed time: {elapsed_time/3600:.1f} hours")
            
            if successful < total * 0.5:  # Less than 50% success rate
                print(f"   ⚠️  Low success rate in this batch - check for issues")
            
            # Check for shutdown signal after batch processing
            if submitter.shutdown_requested:
                print(f"\n🛑 Graceful shutdown completed after batch {batch_num}")
                break
        
        # Final summary
        final_duration = time.time() - start_time
        final_state_stats = submitter.get_submission_stats()
        session_stats = submitter.get_session_stats()
        
        print(f"\n🏁 PROCESSING COMPLETE!")
        print("=" * 60)
        print(f"✅ Successfully processed: {total_successful:,}/{total_processed:,} contracts")
        print(f"📈 Overall success rate: {(total_successful/total_processed*100):.1f}%")
        print(f"⏱️  Total time: {final_duration/3600:.1f} hours")
        print(f"\n📊 Final submission state:")
        print(f"   Total successful: {final_state_stats['successful']:,}")
        print(f"   Total failed: {final_state_stats['failed']:,}")
        print(f"   Success rate: {final_state_stats['success_rate']:.1f}%")
        print(f"\n📊 Session statistics:")
        print(f"   Contracts skipped (already submitted): {session_stats['total_skipped']:,}")
        print(f"   Contracts processed (new submissions): {session_stats['total_processed']:,}")
        print(f"   Skip rate: {session_stats['skip_rate']:.1f}%")
        
        if config['submit_onchain']:
            print(f"🔗 Onchain submissions created attestations on Base")
        else:
            print(f"💾 Offchain submissions stored in OLI database")
            
        if submitter.shutdown_requested:
            print(f"\n⏸️  Processing stopped gracefully")
        else:
            print(f"\n🎉 All Sourcify verified contracts have been submitted to OLI!")
            # Clear checkpoint on successful completion
            submitter.clear_checkpoint()
        
        print(f"💾 State saved to: {submitter.db_path}")
        print(f"📋 Live failed contracts log: {submitter.failed_csv_path}")
        
        # Export failed contracts summary if any
        if final_state_stats['failed'] > 0:
            failed_file = submitter.export_failed_contracts()
            print(f"📋 Failed contracts summary: {failed_file}")
        
    except KeyboardInterrupt:
        print(f"\n\n⏸️  Processing interrupted by user")
        print(f"Progress: {total_successful:,}/{total_processed:,} contracts processed")
        current_stats = submitter.get_submission_stats()
        print(f"💾 State saved - you can resume later")
        print(f"   Successful so far: {current_stats['successful']:,}")
        print(f"   Failed so far: {current_stats['failed']:,}")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Processing failed: {e}")
        print(f"Progress: {total_successful:,}/{total_processed:,} contracts processed")
        current_stats = submitter.get_submission_stats()
        print(f"💾 State saved - you can resume later")
        print(f"   Successful so far: {current_stats['successful']:,}")
        print(f"   Failed so far: {current_stats['failed']:,}")
        sys.exit(1)


if __name__ == "__main__":
    main()