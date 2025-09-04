# CKAN CLI - Data Explorer

A powerful command-line tool to explore and fetch detailed data from the CKAN portal.

## Installation & Setup

```bash
# Dependencies are already installed
# Just run the CLI directly:
python ckan_cli.py --help
```

## Available Commands

### 1. **stats** - Portal Statistics
```bash
python ckan_cli.py stats
```
Shows total packages, organizations, tags, and CKAN version.

### 2. **search** - Search Datasets
```bash
# Basic search
python ckan_cli.py search "climate"

# Search with filters
python ckan_cli.py search "data" -n 20  # Show 20 results
python ckan_cli.py search "air" -o demenech-testing  # Filter by organization
python ckan_cli.py search "*" -t "EV,Population"  # Filter by tags
```

### 3. **show** - Dataset Details
```bash
# Basic details
python ckan_cli.py show idb--climate-finance-dataset

# Full details including custom fields
python ckan_cli.py show idb--climate-finance-dataset -d
```

### 4. **list** - List Entities
```bash
# List packages
python ckan_cli.py list packages -n 20

# List organizations with dataset counts
python ckan_cli.py list orgs -n 50

# List all tags
python ckan_cli.py list tags -n 100
```

### 5. **org** - Organization Details
```bash
# Show organization and its datasets
python ckan_cli.py org user
python ckan_cli.py org idb
```

### 6. **my** - Your Organization's Datasets
```bash
# Default organization (user)
python ckan_cli.py my

# Specific organization
python ckan_cli.py my demenech-testing
```

### 7. **resource** - Resource Operations
```bash
# Get resource details
python ckan_cli.py resource RESOURCE_ID

# Download a resource
python ckan_cli.py resource RESOURCE_ID -d output.csv
```

## Example Workflows

### Find and Explore Climate Data
```bash
# 1. Search for climate-related datasets
python ckan_cli.py search climate -n 10

# 2. Get details of a specific dataset
python ckan_cli.py show idb--climate-finance-dataset

# 3. Explore the organization
python ckan_cli.py org idb
```

### Browse Your Organization's Data
```bash
# 1. List my datasets
python ckan_cli.py my abeelha

# 2. Get detailed info on each
python ckan_cli.py show abeelha--world-happiness-dataset-2020
```

### Discover Popular Topics
```bash
# 1. List popular tags
python ckan_cli.py list tags -n 50

# 2. Search by tag
python ckan_cli.py search "*" -t "Population,Growth"
```

### Find Active Organizations
```bash
# 1. List organizations with dataset counts
python ckan_cli.py list orgs -n 100

# 2. Explore active organization
python ckan_cli.py org demenech-testing
```

## Tested Commands - Exact Examples

These are the exact commands that were tested and work with the current portal:

### View Help
```bash
python ckan_cli.py --help
```

### Get Portal Statistics
```bash
python ckan_cli.py stats
```
Output: Shows 451 packages, 395 organizations, 161 tags, CKAN version 2.11.3

### Show Your Organization's Datasets
```bash
python ckan_cli.py my abeelha
```
Output: Lists 3 datasets from abeelha organization (GitHub Issues, Solar Power Plant, World Happiness)

### Search for Air Quality Data
```bash
python ckan_cli.py search "air quality" -n 5
```
Output: Found 2 air quality datasets from demenech-testing organization

### Search for Climate Data
```bash
python ckan_cli.py search climate -n 3
```
Output: Found 7 climate-related datasets, showing first 3

### Show Dataset Details
```bash
python ckan_cli.py show idb--climate-finance-dataset
```
Output: Complete details of climate finance dataset with 2 CSV resources

### Show Dataset with Full Details
```bash
python ckan_cli.py show demenech-testing--air-quality -d
```
Note: Use -d flag for extended metadata and custom fields

### List Organizations with Package Counts
```bash
python ckan_cli.py list orgs -n 10
```
Output: First 10 organizations with their dataset counts (e.g., i have 3 datasets)

## Output Examples

### Dataset Details Output
```
============================================================
📦 PACKAGE: Dataset climate
============================================================
ID/Name: idb--climate-finance-dataset
Title: Dataset climate
Organization: IDB
Author: Climate Finance
License: None
State: active
Created: 2024-03-15

Description:
  Test dataset

📁 RESOURCES (2 files):
----------------------------------------
  Resource 1:
    Name: Climate Finance Dataset
    Format: CSV
    URL: https://blob.datopian.com/...
    Description: Main dataset

  Resource 2:
    Name: Climate Finance Dataset Dictionary
    Format: CSV
    URL: https://blob.datopian.com/...
```

### Search Results Output
```
📊 Found 7 total results, showing 3:

1. Dataset climate
   Name: idb--climate-finance-dataset
   Org: IDB
   Resources: 2
   Modified: 2024-03-15

2. Featured dataset Example 1
   Name: featured-dataset-1
   Org: mango20
   Resources: 2
   Modified: 2025-03-15
```

## Available Data

- **451 total datasets** available
- **395 organizations** hosting data
- **161 tags** for categorization
- Various formats: CSV, JSON, XML, PDF, etc.

## Tips

1. Use `*` as search query to see all datasets
2. Combine filters for precise searches
3. Use `-d` flag with `show` for complete metadata
4. Organizations often group related datasets
5. Check resource formats before downloading

## No API Key Required

The portal allows public read access for browsing and downloading data.