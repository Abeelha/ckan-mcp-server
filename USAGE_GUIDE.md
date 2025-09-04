# CKAN MCP Server Usage Guide

## Quick Start

The CKAN MCP Server is working and connected to `ENV URL`

### Running the Demo
```bash
python test_ckan_demo.py
```

### Running the MCP Server
```bash
python mcp_ckan_server.py
```

## Available MCP Tools

When the MCP server is running, these tools are available to MCP clients:

### 1. **ckan_package_list**
Lists all packages/datasets in the CKAN portal.
- Parameters: `limit`, `offset` for pagination
- Example: Gets first 100 packages

### 2. **ckan_package_show**
Shows detailed information about a specific package.
- Required: `id` (package name or ID)
- Returns: Full package metadata including resources, organization, tags, etc.

### 3. **ckan_package_search**
Searches for packages using queries.
- Parameters:
  - `q`: Search query (default: "*:*" for all)
  - `fq`: Filter query
  - `sort`: Sort field (e.g., "score desc")
  - `rows`: Number of results (default: 10)
  - `start`: Offset for pagination
- Returns: Search results with count and matching packages

### 4. **ckan_organization_list**
Lists all organizations in the portal.
- Parameters: `all_fields` (boolean) to get full details
- Currently 395 organizations available

### 5. **ckan_organization_show**
Shows details of a specific organization.
- Required: `id` (organization name or ID)
- Parameters: `include_datasets` (boolean)

### 6. **ckan_group_list**
Lists all groups in the portal.
- Parameters: `all_fields` (boolean)

### 7. **ckan_tag_list**
Lists all tags used in the portal.
- Parameters: `vocabulary_id` for filtering
- Currently 161 tags available

### 8. **ckan_resource_show**
Shows details of a specific resource/file.
- Required: `id` (resource ID)

### 9. **ckan_site_read**
Gets site information and statistics.

### 10. **ckan_status_show**
Shows CKAN site status and version information.
- Current version: CKAN 2.11.3

## What You Can Do

### Browse Datasets
- Search by keywords, tags, or organizations
- Get detailed metadata for any dataset
- Access resource URLs and formats

### Explore Organizations
- View organization details and their datasets
- Examples: abeelha, datopian, abc-company, etc.

### Search Capabilities
- Full-text search across all datasets
- Filter by organization, tags, or custom fields
- Sort by relevance, date, or other criteria

### Access Resources
- View file formats (CSV, JSON, XML, etc.)
- Get download URLs for datasets
- Check file sizes and last modified dates

## Example Commands (for MCP clients)

1. **Find datasets about climate:**
   ```json
   {
     "tool": "ckan_package_search",
     "arguments": {
       "q": "climate",
       "rows": 10
     }
   }
   ```

2. **Get all datasets from organization "user":**
   ```json
   {
     "tool": "ckan_organization_show",
     "arguments": {
       "id": "user",
       "include_datasets": true
     }
   }
   ```

3. **List popular tags:**
   ```json
   {
     "tool": "ckan_tag_list",
     "arguments": {}
   }
   ```

## Integration with Claude Desktop

To use with Claude Desktop, add to your config:

```json
{
  "mcpServers": {
    "ckan": {
      "command": "python",
      "args": ["/ckan-mcp-server/mcp_ckan_server.py"],
      "env": {
        "CKAN_URL": "YOUR_URL"
      }
    }
  }
}
```

## No API Key Required

The portal at `YOUR_URL` allows public read access, so no API key is needed for browsing and searching datasets.