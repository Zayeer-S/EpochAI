import os
import re
import time
import hashlib
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple 

from epochai.common.database.database import get_database
from epochai.common.database.dao.track_schema_migrations_dao import TrackSchemaMigrationsDAO
from epochai.common.logging_config import get_logger

class MigrationRunner:
    
    def __init__(
        self,
        migrations_directory: Optional[str] = None
    ):
        self.db = get_database()
        self.migration_dao = TrackSchemaMigrationsDAO()
        self.logger = get_logger(__name__)
        
        if migrations_directory:
            self.migrations_dir = Path(migrations_directory)
        else:
            current_dir = Path(__file__).parent
            self.migrations_dir = current_dir / "migrations"
        
    def _calculate_file_checksum(
        self,
        filepath: Path
    ) -> str:
        """Calculates checksum of migraiton file"""
        
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()
                return hashlib.md5(content).hexdigest()
            
        except Exception as general_error:
            self.logger.error(F"Error calculating checksum for {filepath}: {general_error}")
            return ""
        
    def _parse_migration_filename(
        self,
        filename: str
    ) -> Optional[Tuple[str, str]]:
        """Parses migration filename to get version and description"""
        
        pattern = r'^(\d{3})_(.+)\.sql$'
        
        match = re.match(pattern, filename)
        
        if match:
            version = match.group(1)
            description = match.group(2).replace('_', ' ')
            return version, description
        
        return None
    
    def _discover_migration_files(self) -> List[Tuple[str, str, Path]]:
        """Discovers all migration files in the migrations directory"""
        migration_files= []
        
        if not self.migrations_dir.exists():
            self.logger.warning(f"Migrations directory does not exists, migrations_dir: {self.migrations_dir}")
            return migration_files
        
        for filepath in self.migrations_dir.glob("*.sql"):
            parsed = self._parse_migration_filename(filepath.name)
            if parsed:
                version, description = parsed
                migration_files.append((version, description, filepath))
            else:
                self.logger.warning(F"Skipping file with invalid naming format: {filepath.name}")
                
        migration_files.sort(key=lambda x: x[0])
        
        self.logger.info(f"Discovered {len(migration_files)} migration files")
        return migration_files
    
    def _read_single_migration_file(
        self,
        filepath: Path
        ) -> str:
        """Reads and returns content of a migration file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                return file.read()
            
        except Exception as general_error:
            self.logger.error(f"Error reading migration file '{filepath}': {general_error}")
            return ''
            
    def _execute_migration_sql(
        self,
        sql_content: str
    ) -> Tuple[bool, Optional[str]]:
        """Executes migration SQL content"""
        
        try:
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            with self.db.get_cursor() as cursor:
                for statement in statements:
                    if statement.upper().startswith('COMMENT'):
                        cursor.execute(statement)
                    elif statement:
                        cursor.execute(statement)
                        
                self.db._connection.commit()
                return True, None
            
        except Exception as general_error:
            self.logger.error(f"Error executing migration SQL: {general_error}")
            if self.db._connection:
                self.db._connection.rollback()
            return False, str(general_error)
        
    def get_pending_migrations(self) -> List[Tuple[str, str, Path]]:
        """Gets list of pending unapplied migrations"""
        
        all_migrations = self._discover_migration_files()
        applied_versions = self.migration_dao.get_applied_migration_versions()
        
        pending = [
            (version, description, filepath)
            for version, description, filepath in all_migrations
            if version not in applied_versions
        ]
        
        self.logger.info(f"Found {len(pending)} pending migrations")
        return pending
    
    def run_single_migration(
        self,
        version: str,
        description: str,
        filepath: Path
    ) -> bool:
        """Runs a single migration returning True if successful"""
        
        self.logger.info(f"Running migration {version}: {description}")
        
        start_time = time.time()
        checksum = self._calculate_file_checksum(filepath)
        
        try:
            sql_content = self._read_single_migration_file(filepath)
            
            success, error_message = self._execute_migration_sql(sql_content)
            
            execution_time = time.time() - start_time
            
            if success:
                migration_id = self.migration_dao.create_migration_record(
                    version=version,
                    filename=filepath.name,
                    executed_at=datetime.now(),
                    execution_time_seconds=execution_time,
                    status='completed',
                    checksum=checksum
                )
                
                if migration_id:
                    self.logger.info(f"Successfully apllied migration {version} ({execution_time:.3fs} seconds)")
                    return True
                else:
                    self.logger.error(f"Failed to record migration {version} in tracking table")
                    return False
                
            else:   
                self.migration_dao.create_migration_record(
                    version=version,
                    filename=filepath.name,
                    executed_at=datetime.now(),
                    execution_time_seconds=execution_time,
                    status='failed',
                    checksum=checksum,
                    error_message=error_message
                )
                
                self.logger.error(f"Migration {version} failed: {error_message}")
                return False
                
        except Exception as general_error:
            execution_time = time.time() - start_time
            self.logger.error(f"Unexpected error when running migration {version}: {general_error}")
            
            try:
                self.migration_dao.create_migration_record(
                    version=version,
                    filename=filepath.name,
                    executed_at=datetime.now(),
                    execution_time_seconds=execution_time,
                    status='unexpected_error',
                    checksum=checksum,
                    error_message=str(general_error)
                )
            except Exception as record_error:
                self.logger.error(f"Failed to record migration error: {record_error}")
                
            return False
        
    def run_pending_migrations(
        self,
        dry_run: bool = False
    ) -> Tuple[int, int]:
        """Runs all pending migrations returning [successful_count, failed_count]"""
        
        pending_migrations = self.get_pending_migrations()
        
        if not pending_migrations:
            self.logger.info(f"No pending migrations to run")
            return 0, 0
        
        if dry_run:
            self.logger.info("DRY RUN - Following migrations will be executed:")
            for version, description, filepath in pending_migrations:
                self.logger.info(f"    {version}:{description}:{filepath.name}")
            return 0, 0
        
        successful_count, failed_count = 0, 0
        
        self.logger.info(f"Running  {len(pending_migrations)} pending migrations")
        
        for version, description, filepath in pending_migrations:
            if self.run_single_migration(version, description, filepath):
                successful_count += 1
            else:
                failed_count += 1
                self.logger.error(f"Migration {version} failed, stopping migration run")
                break
            
        self.logger.info(f"Migration run completed, successful: {successful_count}, failed: {failed_count}")
        return successful_count, failed_count
    
    def rollback_migration(
        self,
        version: str
    ) -> bool:
        """
        Mark as migration as rolled back in the tracking table
        
        Note:
            DB still needs manual changes
        """
        
        self.logger.info(f"Rolling back migration {version}")
        
        success = self.migration_dao.mark_migration_rolled_back(version)
        
        if success:
            self.logger.info(f"Successfully marked migration {version} as rolled back")
        else:
            self.logger.error(f"Failed to rollback migration {version}")
            
        return success
    
    def get_all_migrations_status(self) -> Dict:
        """Gets status of all migrations"""
        
        all_migrations = self._discover_migration_files()
        applied_versions = self.migration_dao.get_applied_migration_versions()
        failed_migrations = self.migration_dao.get_failed_migrations()
        
        status = {
            'total_discovered_files': len(all_migrations),
            'total_applied': len(applied_versions),
            'total_pending': len(all_migrations) - len(applied_versions),
            'total_failed': len(failed_migrations),
            'discovered_files': [
                {
                    'version': version,
                    'description': description,
                    'filename': filepath.name,
                    'applied': version in applied_versions
                }
                for version, description, filepath in all_migrations
            ],
            'failed_migrations': [
                {
                    'version': migration.version,
                    'filename': migration.filename,
                    'error_message': migration.error_message,
                    'executed_at': migration.executed_at
                }
                for migration in failed_migrations
            ]
        }
        
        return status
    
def main():
    parser = argparse.ArgumentParser(description="Database Migration Runner")
    parser.add_argument('command', choices=['run', 'status', 'rollback'], help='Commands to execute')
    parser.add_argument('--dry-run', action='store_true', help="Shows what would be done in a migration but doesn't actually execute it")
    parser.add_argument('--description', type=str, help="Description for new migration")
    parser.add_argument('--version', type=str, help='Version to rollback')
    parser.add_argument('--migrations-dir', type=str, help='Path to migrations directory')
    
    args = parser.parse_args()
    
    runner = MigrationRunner(args.migrations_dir)
    
    try:
        if args.command == 'run':
            successful, failed = runner.run_pending_migrations(dry_run=args.dry_run)
            if failed > 0:
                exit(1)
            
        elif args.command == 'status':
            status = runner.get_all_migrations_status()
            
            print(f"Total migrations discovered: {status['total_discovered_files']}")
            print(f"Total applied: {status['total_applied']}")
            print(f"Total pending: {status['total_pending']}")
            print(f"Total failed: {status['total_failed']}")
            
            if status['total_pending'] > 0:
                print("\nPending migrations:")
                for migration in status['discovered_files']:
                    if not migration['applied']:
                        print(f"  {migration['version']}: {migration['description']}")
            
            if status['failed_migrations']:
                print("\nFailed migrations:")
                for migration in status['failed_migrations']:
                    print(f"  {migration['version']}: {migration['error_message']}")
                    
        elif args.command == 'rollback':
            if not args.version:
                print("Error: --version is required for rollback command")
                exit(1)
            success = runner.rollback_migration(args.version)
            if not success:
                exit(1)
                
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()