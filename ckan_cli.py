#!/usr/bin/env python3

import asyncio
import json
import sys
import argparse
import os
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from mcp_ckan_server import CKANAPIClient
import aiohttp

# Load environment variables
load_dotenv()

class CKANExplorer:
    """Interactive CKAN data explorer CLI"""
    
    def __init__(self):
        self.ckan_url = os.getenv("CKAN_URL", "https://api.cloud.portaljs.com/")
        self.client = None
        
    async def __aenter__(self):
        self.client = await CKANAPIClient(self.ckan_url).__aenter__()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def search_packages(self, query: str = "*:*", rows: int = 10, 
                            organization: Optional[str] = None,
                            tags: Optional[str] = None):
        """Search for packages with filters"""
        fq_parts = []
        if organization:
            fq_parts.append(f"organization:{organization}")
        if tags:
            for tag in tags.split(","):
                fq_parts.append(f"tags:{tag.strip()}")
        
        fq = " AND ".join(fq_parts) if fq_parts else None
        
        params = {"q": query, "rows": rows}
        if fq:
            params["fq"] = fq
            
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        result = await self.client._make_request("GET", f"package_search?{query_string}")
        return result
    
    async def get_package_details(self, package_id: str):
        """Get complete package details including all resources"""
        result = await self.client._make_request("GET", f"package_show?id={package_id}")
        return result
    
    async def get_resource_details(self, resource_id: str):
        """Get resource details and optionally download URL"""
        result = await self.client._make_request("GET", f"resource_show?id={resource_id}")
        return result
    
    async def download_resource(self, resource_url: str, filename: str):
        """Download a resource file"""
        async with aiohttp.ClientSession() as session:
            async with session.get(resource_url) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(filename, 'wb') as f:
                        f.write(content)
                    return True
                return False
    
    async def list_organization_datasets(self, org_id: str):
        """List all datasets from an organization"""
        result = await self.client._make_request("GET", 
                                                f"organization_show?id={org_id}&include_datasets=true")
        return result
    
    async def get_statistics(self):
        """Get portal statistics"""
        stats = {}
        
        # Get total packages
        search = await self.client._make_request("GET", "package_search?q=*:*&rows=1")
        stats['total_packages'] = search.get('count', 0)
        
        # Get organizations count
        orgs = await self.client._make_request("GET", "organization_list")
        stats['total_organizations'] = len(orgs)
        
        # Get tags count
        tags = await self.client._make_request("GET", "tag_list")
        stats['total_tags'] = len(tags)
        
        # Get site info
        site = await self.client._make_request("GET", "status_show")
        stats['ckan_version'] = site.get('ckan_version', 'Unknown')
        
        return stats

def format_package_display(pkg: Dict[str, Any], detailed: bool = False) -> str:
    """Format package data for display"""
    if not pkg:
        return "❌ Package not found or no data available"
    
    output = []
    output.append(f"\n{'='*60}")
    output.append(f"📦 PACKAGE: {pkg.get('title', pkg.get('name', 'Unknown'))}")
    output.append(f"{'='*60}")
    
    # Basic info
    output.append(f"ID/Name: {pkg.get('name', 'N/A')}")
    output.append(f"Title: {pkg.get('title', 'N/A')}")
    output.append(f"Organization: {pkg.get('organization', {}).get('title', 'N/A')}")
    output.append(f"Author: {pkg.get('author', 'N/A')}")
    output.append(f"Maintainer: {pkg.get('maintainer', 'N/A')}")
    output.append(f"License: {pkg.get('license_title', 'N/A')}")
    output.append(f"State: {pkg.get('state', 'N/A')}")
    
    # Dates
    created = pkg.get('metadata_created', '')
    modified = pkg.get('metadata_modified', '')
    if created:
        output.append(f"Created: {created[:10]}")
    if modified:
        output.append(f"Modified: {modified[:10]}")
    
    # Description
    if pkg.get('notes'):
        output.append(f"\nDescription:")
        output.append(f"  {pkg.get('notes', '')[:500]}")
    
    # Tags
    if pkg.get('tags'):
        tags = [tag.get('display_name', tag.get('name', '')) for tag in pkg['tags']]
        output.append(f"\nTags: {', '.join(tags)}")
    
    # Resources
    resources = pkg.get('resources', [])
    if resources:
        output.append(f"\n📁 RESOURCES ({len(resources)} files):")
        output.append("-" * 40)
        for i, res in enumerate(resources, 1):
            output.append(f"\n  Resource {i}:")
            output.append(f"    Name: {res.get('name', res.get('url', 'N/A'))}")
            output.append(f"    Format: {res.get('format', 'N/A')}")
            output.append(f"    Size: {format_size(res.get('size', 0))}")
            output.append(f"    URL: {res.get('url', 'N/A')}")
            if res.get('description'):
                output.append(f"    Description: {res['description'][:200]}")
            if detailed:
                output.append(f"    ID: {res.get('id', 'N/A')}")
                output.append(f"    Created: {res.get('created', 'N/A')[:10]}")
                output.append(f"    Modified: {res.get('last_modified', 'N/A')[:10]}")
    
    # Extras (custom fields)
    if detailed and pkg.get('extras'):
        output.append(f"\n📋 CUSTOM FIELDS:")
        output.append("-" * 40)
        for extra in pkg['extras']:
            output.append(f"  {extra.get('key', 'N/A')}: {extra.get('value', 'N/A')}")
    
    return "\n".join(output)

def format_size(size_bytes):
    """Format bytes to human readable size"""
    if not size_bytes:
        return "Unknown"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

async def main():
    parser = argparse.ArgumentParser(description='CKAN Portal Data Explorer CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for datasets')
    search_parser.add_argument('query', nargs='?', default='*:*', help='Search query')
    search_parser.add_argument('-n', '--rows', type=int, default=10, help='Number of results')
    search_parser.add_argument('-o', '--org', help='Filter by organization')
    search_parser.add_argument('-t', '--tags', help='Filter by tags (comma-separated)')
    
    # Show command
    show_parser = subparsers.add_parser('show', help='Show detailed package info')
    show_parser.add_argument('package_id', help='Package ID or name')
    show_parser.add_argument('-d', '--detailed', action='store_true', help='Show all details')
    
    # Resource command
    resource_parser = subparsers.add_parser('resource', help='Get resource details')
    resource_parser.add_argument('resource_id', help='Resource ID')
    resource_parser.add_argument('-d', '--download', help='Download to filename')
    
    # Organization command
    org_parser = subparsers.add_parser('org', help='List organization datasets')
    org_parser.add_argument('org_id', help='Organization ID or name')
    
    # List commands
    list_parser = subparsers.add_parser('list', help='List entities')
    list_parser.add_argument('entity', choices=['packages', 'orgs', 'tags'], 
                           help='What to list')
    list_parser.add_argument('-n', '--number', type=int, default=20, help='Number to show')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show portal statistics')
    
    # Your datasets command
    my_parser = subparsers.add_parser('my', help='Show your organization datasets')
    my_parser.add_argument('org', default='abeelha', nargs='?', help='Your organization')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        async with CKANExplorer() as explorer:
            print(f"🌐 Connected to: {explorer.ckan_url}")
            print("-" * 60)
            
            if args.command == 'search':
                print(f"🔍 Searching for: '{args.query}'")
                if args.org:
                    print(f"   Organization filter: {args.org}")
                if args.tags:
                    print(f"   Tags filter: {args.tags}")
                
                results = await explorer.search_packages(
                    args.query, args.rows, args.org, args.tags
                )
                
                count = results.get('count', 0)
                print(f"\n📊 Found {count} total results, showing {min(args.rows, count)}:\n")
                
                for i, pkg in enumerate(results.get('results', []), 1):
                    print(f"{i}. {pkg.get('title', pkg.get('name', 'Unknown'))}")
                    print(f"   Name: {pkg.get('name', 'N/A')}")
                    print(f"   Org: {pkg.get('organization', {}).get('title', 'N/A')}")
                    print(f"   Resources: {len(pkg.get('resources', []))}")
                    print(f"   Modified: {pkg.get('metadata_modified', 'N/A')[:10]}")
                    print()
            
            elif args.command == 'show':
                print(f"📄 Getting details for: {args.package_id}")
                pkg = await explorer.get_package_details(args.package_id)
                print(format_package_display(pkg, detailed=args.detailed))
            
            elif args.command == 'resource':
                print(f"📎 Getting resource: {args.resource_id}")
                res = await explorer.get_resource_details(args.resource_id)
                
                print(f"\nResource Details:")
                print(f"  Name: {res.get('name', 'N/A')}")
                print(f"  Format: {res.get('format', 'N/A')}")
                print(f"  Size: {format_size(res.get('size', 0))}")
                print(f"  URL: {res.get('url', 'N/A')}")
                print(f"  Package: {res.get('package_id', 'N/A')}")
                print(f"  Created: {res.get('created', 'N/A')[:19]}")
                print(f"  Modified: {res.get('last_modified', 'N/A')[:19]}")
                
                if args.download and res.get('url'):
                    print(f"\n⬇️  Downloading to: {args.download}")
                    success = await explorer.download_resource(res['url'], args.download)
                    if success:
                        print("✅ Download complete!")
                    else:
                        print("❌ Download failed!")
            
            elif args.command == 'org':
                print(f"🏢 Organization: {args.org_id}")
                org = await explorer.list_organization_datasets(args.org_id)
                
                print(f"\nOrganization: {org.get('title', org.get('name', 'Unknown'))}")
                print(f"Display Name: {org.get('display_name', 'N/A')}")
                print(f"Description: {org.get('description', 'N/A')[:200]}")
                print(f"Created: {org.get('created', 'N/A')[:10]}")
                print(f"Package Count: {org.get('package_count', 0)}")
                
                packages = org.get('packages', [])
                if packages:
                    print(f"\n📦 Datasets ({len(packages)}):")
                    for i, pkg in enumerate(packages[:20], 1):
                        print(f"  {i}. {pkg.get('title', pkg.get('name', 'Unknown'))}")
            
            elif args.command == 'list':
                if args.entity == 'packages':
                    print(f"📦 Listing packages (first {args.number}):")
                    pkgs = await explorer.client._make_request("GET", 
                                                              f"package_list?limit={args.number}")
                    for i, pkg in enumerate(pkgs, 1):
                        print(f"  {i}. {pkg}")
                
                elif args.entity == 'orgs':
                    print(f"🏢 Listing organizations:")
                    orgs = await explorer.client._make_request("GET", 
                                                              "organization_list?all_fields=true")
                    for i, org in enumerate(orgs[:args.number], 1):
                        if isinstance(org, dict):
                            print(f"  {i}. {org.get('display_name', org.get('name', 'Unknown'))} ({org.get('package_count', 0)} datasets)")
                        else:
                            print(f"  {i}. {org}")
                
                elif args.entity == 'tags':
                    print(f"🏷️  Listing tags:")
                    tags = await explorer.client._make_request("GET", "tag_list")
                    for i, tag in enumerate(tags[:args.number], 1):
                        print(f"  {i}. {tag}")
            
            elif args.command == 'stats':
                print("📊 Portal Statistics")
                stats = await explorer.get_statistics()
                print(f"  Total Packages: {stats['total_packages']}")
                print(f"  Total Organizations: {stats['total_organizations']}")
                print(f"  Total Tags: {stats['total_tags']}")
                print(f"  CKAN Version: {stats['ckan_version']}")
            
            elif args.command == 'my':
                print(f"🏢 Your datasets (org: {args.org}):")
                try:
                    org = await explorer.list_organization_datasets(args.org)
                    packages = org.get('packages', [])
                    
                    if packages:
                        print(f"\nFound {len(packages)} datasets in '{args.org}':\n")
                        for i, pkg in enumerate(packages, 1):
                            print(f"{i}. {pkg.get('title', pkg.get('name', 'Unknown'))}")
                            print(f"   Name: {pkg.get('name', 'N/A')}")
                            print(f"   Resources: {len(pkg.get('resources', []))} files")
                            print(f"   Modified: {pkg.get('metadata_modified', 'N/A')[:10]}")
                            print()
                    else:
                        print("No datasets found in this organization")
                except:
                    print(f"Organization '{args.org}' not found or no access")
                    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())