#!/bin/bash
# Export local PostgreSQL data for cloud deployment
# Usage: bash scripts/export_db_for_cloud.sh

echo '=== Exporting steel_db from local Docker container ==='
docker exec steel-postgres pg_dump   -U steel_admin   --no-owner   --no-acl   steel_db > steel_db_cloud_export.sql

echo ''
echo '=== Done! File: steel_db_cloud_export.sql ==='
echo ''
echo 'To restore to Neon / Render PostgreSQL:'
echo '  psql "<YOUR_DATABASE_URL>" < steel_db_cloud_export.sql'
echo ''
echo 'Get your free PostgreSQL at: https://neon.tech'
