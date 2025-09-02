#!/usr/bin/env python3
"""
Download Sourcify parquet files locally for fast processing.

This script downloads all necessary parquet files from Sourcify's public export
to enable fast local processing with DuckDB.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib


def check_virtual_environment():
    """Check if running in a virtual environment and warn if not."""
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
        
        try:
            response = input("Continue anyway? [y/N]: ").lower().strip()
            if response not in ['y', 'yes']:
                print("Exiting. Please setup virtual environment first.")
                sys.exit(1)
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(1)


class SourceifyParquetDownloader:
    """Download Sourcify parquet files for local processing."""
    
    def __init__(self, base_url: str = "https://export.sourcify.dev", data_dir: str = "./sourcify_data"):
        """
        Initialize the downloader.
        
        Args:
            base_url: Base URL for Sourcify exports
            data_dir: Local directory to store downloaded files
        """
        self.base_url = base_url
        self.data_dir = Path(data_dir)
        self.logger = self._setup_logger()
        
        # Create data directory
        self.data_dir.mkdir(exist_ok=True)
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger('SourceifyDownloader')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
        
    def get_manifest(self) -> Dict:
        """Download and parse the manifest file."""
        try:
            self.logger.info("Downloading manifest file...")
            response = requests.get(f"{self.base_url}/manifest.json", timeout=30)
            response.raise_for_status()
            
            manifest = response.json()
            
            # Save manifest locally
            manifest_path = self.data_dir / "manifest.json"
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
                
            self.logger.info(f"Manifest saved to {manifest_path}")
            self.logger.info(f"Export date: {manifest.get('dateStr', 'Unknown')}")
            
            return manifest
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to download manifest: {e}")
            raise
            
    def get_file_info(self, manifest: Dict) -> Dict[str, List[str]]:
        """Extract file information for required tables."""
        required_tables = ['verified_contracts', 'contract_deployments', 'compiled_contracts']
        
        file_info = {}
        total_files = 0
        
        for table_name in required_tables:
            files = manifest.get('files', {}).get(table_name, [])
            if files:
                file_info[table_name] = files
                total_files += len(files)
                self.logger.info(f"{table_name}: {len(files)} files")
            else:
                self.logger.warning(f"No files found for {table_name}")
                
        self.logger.info(f"Total files to download: {total_files}")
        return file_info
        
    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
        
    def download_file(self, file_path: str, table_name: str) -> bool:
        """
        Download a single parquet file.
        
        Args:
            file_path: Relative file path from manifest
            table_name: Table name for organizing files
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            url = f"{self.base_url}/{file_path}"
            filename = Path(file_path).name
            
            # Create table subdirectory
            table_dir = self.data_dir / table_name
            table_dir.mkdir(exist_ok=True)
            
            local_path = table_dir / filename
            
            # Check if file already exists and is complete
            if local_path.exists():
                try:
                    # Quick check - try to get file size from server
                    head_response = requests.head(url, timeout=10)
                    if head_response.ok:
                        server_size = int(head_response.headers.get('content-length', 0))
                        local_size = local_path.stat().st_size
                        
                        if server_size > 0 and local_size == server_size:
                            self.logger.debug(f"Skipping {filename} (already exists)")
                            return True
                except:
                    pass  # If check fails, re-download
                    
            # Download the file
            self.logger.info(f"Downloading {filename}...")
            start_time = time.time()
            
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # Write file in chunks
            with open(local_path, 'wb') as f:
                downloaded = 0
                total_size = int(response.headers.get('content-length', 0))
                
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Progress indicator for large files
                        if total_size > 0 and downloaded % (1024 * 1024 * 10) == 0:  # Every 10MB
                            progress = (downloaded / total_size) * 100
                            self.logger.debug(f"  {filename}: {progress:.1f}% complete")
            
            duration = time.time() - start_time
            file_size_mb = local_path.stat().st_size / (1024 * 1024)
            speed = file_size_mb / duration if duration > 0 else 0
            
            self.logger.info(f"✓ {filename}: {file_size_mb:.1f}MB in {duration:.1f}s ({speed:.1f}MB/s)")
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Failed to download {file_path}: {e}")
            
            # Clean up partial file
            if local_path.exists():
                try:
                    local_path.unlink()
                except:
                    pass
                    
            return False
            
    def download_all_files(self, file_info: Dict[str, List[str]], max_workers: int = 4) -> Dict[str, int]:
        """
        Download all required parquet files using thread pool.
        
        Args:
            file_info: Dictionary mapping table names to file lists
            max_workers: Maximum number of concurrent downloads
            
        Returns:
            Dictionary with download statistics
        """
        stats = {'successful': 0, 'failed': 0, 'total': 0}
        
        # Create download tasks
        download_tasks = []
        for table_name, files in file_info.items():
            for file_path in files:
                download_tasks.append((file_path, table_name))
                
        stats['total'] = len(download_tasks)
        
        self.logger.info(f"Starting download of {stats['total']} files with {max_workers} workers...")
        start_time = time.time()
        
        # Execute downloads in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.download_file, file_path, table_name): (file_path, table_name)
                for file_path, table_name in download_tasks
            }
            
            # Process completed downloads
            for future in as_completed(future_to_task):
                file_path, table_name = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                        
                except Exception as e:
                    self.logger.error(f"Exception downloading {file_path}: {e}")
                    stats['failed'] += 1
                    
                # Progress update
                completed = stats['successful'] + stats['failed']
                if completed % 5 == 0 or completed == stats['total']:
                    self.logger.info(f"Progress: {completed}/{stats['total']} files "
                                   f"({stats['successful']} successful, {stats['failed']} failed)")
        
        duration = time.time() - start_time
        self.logger.info(f"Download completed in {duration:.1f}s")
        self.logger.info(f"Results: {stats['successful']} successful, {stats['failed']} failed")
        
        return stats
        
    def verify_downloads(self, file_info: Dict[str, List[str]]) -> bool:
        """Verify that all files were downloaded successfully."""
        self.logger.info("Verifying downloaded files...")
        
        missing_files = []
        corrupted_files = []
        
        for table_name, files in file_info.items():
            table_dir = self.data_dir / table_name
            
            for file_path in files:
                filename = Path(file_path).name
                local_path = table_dir / filename
                
                if not local_path.exists():
                    missing_files.append(f"{table_name}/{filename}")
                    continue
                    
                # Basic integrity check - file size > 0
                if local_path.stat().st_size == 0:
                    corrupted_files.append(f"{table_name}/{filename}")
                    
        if missing_files:
            self.logger.error(f"Missing files: {missing_files}")
            
        if corrupted_files:
            self.logger.error(f"Corrupted files (0 bytes): {corrupted_files}")
            
        if missing_files or corrupted_files:
            return False
            
        self.logger.info("✓ All files verified successfully")
        return True
        
    def get_download_summary(self) -> Dict:
        """Get summary of downloaded files."""
        summary = {
            'tables': {},
            'total_files': 0,
            'total_size_mb': 0
        }
        
        for table_dir in self.data_dir.iterdir():
            if table_dir.is_dir() and table_dir.name != '__pycache__':
                table_name = table_dir.name
                parquet_files = list(table_dir.glob('*.parquet'))
                
                table_size = sum(f.stat().st_size for f in parquet_files)
                table_size_mb = table_size / (1024 * 1024)
                
                summary['tables'][table_name] = {
                    'file_count': len(parquet_files),
                    'size_mb': round(table_size_mb, 1)
                }
                
                summary['total_files'] += len(parquet_files)
                summary['total_size_mb'] += table_size_mb
                
        summary['total_size_mb'] = round(summary['total_size_mb'], 1)
        return summary


def main():
    """Main function to download Sourcify parquet files."""
    check_virtual_environment()
    
    print("🚀 Sourcify Parquet File Downloader")
    print("=" * 50)
    
    # Configuration
    data_dir = "./sourcify_data"
    max_workers = 4  # Concurrent downloads
    
    print(f"Data directory: {data_dir}")
    print(f"Max concurrent downloads: {max_workers}")
    print()
    
    try:
        # Initialize downloader
        downloader = SourceifyParquetDownloader(data_dir=data_dir)
        
        # Get manifest and file info
        manifest = downloader.get_manifest()
        file_info = downloader.get_file_info(manifest)
        
        if not file_info:
            print("❌ No files found to download")
            return
            
        # Ask for confirmation
        total_files = sum(len(files) for files in file_info.values())
        print(f"\nReady to download {total_files} parquet files")
        print("This will download approximately 10-15GB of data")
        
        try:
            response = input("\nContinue with download? [y/N]: ").lower().strip()
            if response not in ['y', 'yes']:
                print("Download cancelled")
                return
        except (KeyboardInterrupt, EOFError):
            print("\nDownload cancelled")
            return
            
        # Download files
        stats = downloader.download_all_files(file_info, max_workers=max_workers)
        
        # Verify downloads
        if stats['failed'] == 0:
            success = downloader.verify_downloads(file_info)
            if not success:
                print("❌ Download verification failed")
                return
        else:
            print(f"⚠️  {stats['failed']} files failed to download")
            
        # Show summary
        summary = downloader.get_download_summary()
        print(f"\n✅ Download complete!")
        print(f"Total: {summary['total_files']} files, {summary['total_size_mb']}MB")
        
        for table_name, info in summary['tables'].items():
            print(f"  {table_name}: {info['file_count']} files, {info['size_mb']}MB")
            
        print(f"\nFiles saved to: {Path(data_dir).absolute()}")
        print("\nNext steps:")
        print("1. Run: python local_data_processor.py")
        print("2. Process contracts for OLI submission")
        
    except Exception as e:
        print(f"❌ Download failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()