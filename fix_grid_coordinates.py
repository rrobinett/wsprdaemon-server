#!/usr/bin/env python3
"""
Fix Maidenhead Grid Coordinates in ClickHouse Tables
Recalculates rx_lat, rx_lon, tx_lat, tx_lon from grid squares using corrected conversion
"""
import argparse
import sys
import time
from typing import Tuple
import clickhouse_connect
import logging

VERSION = "1.0.1"  # Fixed: Use CASE statements for bulk updates (ClickHouse 25.x compatible)

# Corrected Maidenhead conversion function
def maidenhead_to_latlon(grid: str) -> Tuple[float, float]:
    """Convert Maidenhead grid square to latitude/longitude (center of square)
    
    Returns (lat, lon) with 3 decimal places precision
    Convention: 4-character grids are centered at subsquare 'll' (index 11)
    """
    if not grid or len(grid) < 4:
        return (-999.0, -999.0)
    
    # Only uppercase the field letters (first 2 chars), leave subsquares lowercase
    grid = grid[:2].upper() + grid[2:]
    
    try:
        # Field (first 2 characters): 20° lon, 10° lat
        lon = (ord(grid[0]) - ord('A')) * 20 - 180
        lat = (ord(grid[1]) - ord('A')) * 10 - 90
        
        # Square (next 2 digits): 2° lon, 1° lat
        lon += int(grid[2]) * 2
        lat += int(grid[3]) * 1
        
        if len(grid) >= 6:
            # For 6-character grids, add subsquare offset and center in subsquare
            lon += (ord(grid[4].lower()) - ord('a')) * (2.0/24.0)
            lat += (ord(grid[5].lower()) - ord('a')) * (1.0/24.0)
            lon += (1.0/24.0)
            lat += (0.5/24.0)
        else:
            # For 4-character grids, use center of 'll' subsquare (subsquare index 11)
            lon += 11 * (2.0/24.0) + (1.0/24.0)  # = 23/24 = 0.958
            lat += 11 * (1.0/24.0) + (0.5/24.0)  # = 11.5/24 = 0.479
        
        return (round(lat, 3), round(lon, 3))
        
    except (ValueError, IndexError):
        return (-999.0, -999.0)


def setup_logging(verbose: bool = False):
    """Setup logging"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def log(message: str, level: str = "INFO"):
    """Log a message"""
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
    }
    logging.log(level_map.get(level, logging.INFO), message)


def get_table_count(client, database: str, table: str) -> int:
    """Get total row count from table"""
    try:
        result = client.query(f"SELECT count() FROM {database}.{table}")
        return int(result.result_rows[0][0])
    except Exception as e:
        log(f"Error getting count from {database}.{table}: {e}", "ERROR")
        return 0


def fix_table_coordinates(client, database: str, table: str, batch_size: int, 
                          dry_run: bool, limit: int = None) -> Tuple[int, int, int]:
    """
    Fix coordinates in a table by recalculating from grid squares
    Returns: (total_processed, updated_count, error_count)
    """
    log(f"Processing table: {database}.{table}")
    
    # Get total count
    if limit:
        total_count = min(limit, get_table_count(client, database, table))
    else:
        total_count = get_table_count(client, database, table)
    
    log(f"Total rows to process: {total_count:,}")
    
    if total_count == 0:
        return 0, 0, 0
    
    processed = 0
    updated = 0
    errors = 0
    offset = 0
    
    while offset < total_count:
        # Fetch batch with grid squares and current coordinates
        query = f"""
        SELECT 
            id,
            rx_loc,
            rx_lat,
            rx_lon,
            tx_loc,
            tx_lat,
            tx_lon
        FROM {database}.{table}
        ORDER BY id
        LIMIT {batch_size}
        OFFSET {offset}
        """
        
        try:
            result = client.query(query)
            rows = result.result_rows
            
            if not rows:
                break
            
            # Process each row
            updates = []
            for row in rows:
                id_val, rx_loc, rx_lat_old, rx_lon_old, tx_loc, tx_lat_old, tx_lon_old = row
                
                # Recalculate coordinates
                rx_lat_new, rx_lon_new = maidenhead_to_latlon(rx_loc)
                tx_lat_new, tx_lon_new = maidenhead_to_latlon(tx_loc)
                
                # Check if coordinates changed significantly (>0.001 degree)
                rx_changed = (abs(rx_lat_new - rx_lat_old) > 0.001 or 
                             abs(rx_lon_new - rx_lon_old) > 0.001)
                tx_changed = (abs(tx_lat_new - tx_lat_old) > 0.001 or 
                             abs(tx_lon_new - tx_lon_old) > 0.001)
                
                if rx_changed or tx_changed:
                    updates.append({
                        'id': id_val,
                        'rx_lat': rx_lat_new,
                        'rx_lon': rx_lon_new,
                        'tx_lat': tx_lat_new,
                        'tx_lon': tx_lon_new
                    })
            
            # Apply updates if not dry run
            if updates and not dry_run:
                # Build multiple CASE statements for bulk update
                # This is much more efficient than individual updates
                rx_lat_cases = []
                rx_lon_cases = []
                tx_lat_cases = []
                tx_lon_cases = []
                id_list = []
                
                for u in updates:
                    id_list.append(str(u['id']))
                    rx_lat_cases.append(f"WHEN id = {u['id']} THEN {u['rx_lat']}")
                    rx_lon_cases.append(f"WHEN id = {u['id']} THEN {u['rx_lon']}")
                    tx_lat_cases.append(f"WHEN id = {u['id']} THEN {u['tx_lat']}")
                    tx_lon_cases.append(f"WHEN id = {u['id']} THEN {u['tx_lon']}")
                
                ids_str = ','.join(id_list)
                
                try:
                    update_query = f"""
                    ALTER TABLE {database}.{table}
                    UPDATE 
                        rx_lat = CASE {' '.join(rx_lat_cases)} ELSE rx_lat END,
                        rx_lon = CASE {' '.join(rx_lon_cases)} ELSE rx_lon END,
                        tx_lat = CASE {' '.join(tx_lat_cases)} ELSE tx_lat END,
                        tx_lon = CASE {' '.join(tx_lon_cases)} ELSE tx_lon END
                    WHERE id IN ({ids_str})
                    """
                    client.command(update_query)
                    updated += len(updates)
                except Exception as e:
                    log(f"Error in batch update: {e}", "ERROR")
                    errors += len(updates)
            elif updates and dry_run:
                updated += len(updates)
                # Show a few examples in dry run
                if offset < batch_size * 2:  # First 2 batches
                    for u in updates[:5]:  # First 5 of each batch
                        log(f"  Would update ID {u['id']}: rx=({u['rx_lat']}, {u['rx_lon']}), tx=({u['tx_lat']}, {u['tx_lon']})", "DEBUG")
            
            processed += len(rows)
            offset += len(rows)
            
            # Progress update every 10 batches
            if offset % (batch_size * 10) == 0 or offset >= total_count:
                pct = 100.0 * offset / total_count
                log(f"Progress: {offset:,}/{total_count:,} ({pct:.1f}%) - {updated:,} updated")
            
        except Exception as e:
            log(f"Error processing batch at offset {offset}: {e}", "ERROR")
            errors += batch_size
            offset += batch_size
            continue
    
    return processed, updated, errors


def main():
    parser = argparse.ArgumentParser(
        description='Fix Maidenhead grid coordinates in ClickHouse tables'
    )
    
    parser.add_argument('--clickhouse-host', default='localhost', help='ClickHouse host')
    parser.add_argument('--clickhouse-port', type=int, default=8123, help='ClickHouse port')
    parser.add_argument('--clickhouse-user', required=True, help='ClickHouse user (needs write access)')
    parser.add_argument('--clickhouse-password', required=True, help='ClickHouse password')
    
    parser.add_argument('--wsprnet-database', default='wsprnet', help='WSPRNET database name')
    parser.add_argument('--wsprnet-table', default='spots', help='WSPRNET spots table name')
    parser.add_argument('--wsprdaemon-database', default='wsprdaemon', help='WSPRDAEMON database name')
    parser.add_argument('--wsprdaemon-table', default='spots_extended', help='WSPRDAEMON spots table name')
    
    parser.add_argument('--batch-size', type=int, default=10000, help='Rows per batch')
    parser.add_argument('--limit', type=int, help='Limit total rows to process (for testing)')
    parser.add_argument('--skip-wsprnet', action='store_true', help='Skip wsprnet.spots table')
    parser.add_argument('--skip-wsprdaemon', action='store_true', help='Skip wsprdaemon.spots_extended table')
    
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without making changes')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    log("=" * 70)
    log(f"Maidenhead Grid Coordinate Fix Tool v{VERSION}")
    if args.dry_run:
        log("*** DRY RUN MODE - No changes will be made ***")
    log("=" * 70)
    
    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=args.clickhouse_host,
            port=args.clickhouse_port,
            username=args.clickhouse_user,
            password=args.clickhouse_password
        )
        log(f"Connected to ClickHouse at {args.clickhouse_host}:{args.clickhouse_port}")
    except Exception as e:
        log(f"Failed to connect to ClickHouse: {e}", "ERROR")
        sys.exit(1)
    
    start_time = time.time()
    total_processed = 0
    total_updated = 0
    total_errors = 0
    
    # Process WSPRNET table
    if not args.skip_wsprnet:
        log("")
        log("="*70)
        processed, updated, errors = fix_table_coordinates(
            client,
            args.wsprnet_database,
            args.wsprnet_table,
            args.batch_size,
            args.dry_run,
            args.limit
        )
        total_processed += processed
        total_updated += updated
        total_errors += errors
        log(f"Completed {args.wsprnet_database}.{args.wsprnet_table}: {processed:,} processed, {updated:,} updated, {errors:,} errors")
    
    # Process WSPRDAEMON table
    if not args.skip_wsprdaemon:
        log("")
        log("="*70)
        processed, updated, errors = fix_table_coordinates(
            client,
            args.wsprdaemon_database,
            args.wsprdaemon_table,
            args.batch_size,
            args.dry_run,
            args.limit
        )
        total_processed += processed
        total_updated += updated
        total_errors += errors
        log(f"Completed {args.wsprdaemon_database}.{args.wsprdaemon_table}: {processed:,} processed, {updated:,} updated, {errors:,} errors")
    
    elapsed = time.time() - start_time
    
    log("")
    log("="*70)
    log("SUMMARY")
    log("="*70)
    log(f"Total rows processed: {total_processed:,}")
    log(f"Total rows updated:   {total_updated:,}")
    log(f"Total errors:         {total_errors:,}")
    log(f"Time elapsed:         {elapsed:.1f} seconds")
    log(f"Rows per second:      {total_processed/elapsed:.0f}")
    
    if args.dry_run:
        log("")
        log("This was a dry run. Run without --dry-run to apply changes.")


if __name__ == '__main__':
    main()
