#!/bin/bash

# Install required dependencies
sudo apt-get update
sudo apt-get install -y mongodb-org-tools

# Set backup directory
backup_dir="/path/to/backup/directory"

# Create backup directory if it doesn't exist
mkdir -p "$backup_dir"

# Define backup file name with timestamp
backup_file="$backup_dir/mongo_backup_$(date +%Y%m%d_%H%M%S).gz"

# MongoDB URI
mongo_uri="mongodb://user:pass@mongodb.website.me/"

# Perform MongoDB backup
mongodump --uri="$mongo_uri"


echo "MongoDB backup completed successfully. Backup file: $backup_file.tar.gz"
