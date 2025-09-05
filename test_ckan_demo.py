#!/usr/bin/env python3

import asyncio
import json
import os
from dotenv import load_dotenv
from mcp_ckan_server import CKANAPIClient

# Load environment variables from .env file
load_dotenv()

async def demo_ckan_api():
    """Demonstrate CKAN API functionality with portaljs.com"""
    
    ckan_url = os.getenv("CKAN_URL")
    print(f"Connecting to CKAN API at: {ckan_url}")
    print("=" * 60)

    # Create CKAN client (without API key as requested)
    async with CKANAPIClient(ckan_url) as client:

        # 1. Test site status
        print("\n1. TESTING SITE STATUS")
        print("-" * 40)
        try:
            status = await client._make_request("GET", "status_show")
            print("[OK] API Connection successful!")
            print(f"Site status: {json.dumps(status, indent=2)[:500]}...")
        except Exception as e:
            print(f"[ERROR] Status check failed: {str(e)}")

        # 2. List packages/datasets
        print("\n2. LISTING PACKAGES (First 5)")
        print("-" * 40)
        try:
            packages = await client._make_request("GET", "package_list?limit=5")
            if packages:
                print(f"Found {len(packages)} packages:")
                for i, pkg in enumerate(packages[:5], 1):
                    print(f"  {i}. {pkg}")
            else:
                print("No packages found")
        except Exception as e:
            print(f"[ERROR] Package list failed: {str(e)}")

        # 3. Search for packages
        print("\n3. SEARCHING FOR PACKAGES")
        print("-" * 40)
        try:
            search_results = await client._make_request("GET", "package_search?q=*:*&rows=3")
            count = search_results.get('count', 0)
            print(f"Total packages found: {count}")

            if search_results.get('results'):
                print("\nFirst 3 packages:")
                for i, pkg in enumerate(search_results['results'], 1):
                    print(f"\n  Package {i}:")
                    print(f"    Name: {pkg.get('name', 'N/A')}")
                    print(f"    Title: {pkg.get('title', 'N/A')}")
                    print(f"    Organization: {pkg.get('organization', {}).get('name', 'N/A')}")
                    print(f"    Resources: {len(pkg.get('resources', []))} file(s)")
                    print(f"    Created: {pkg.get('metadata_created', 'N/A')[:10]}")
        except Exception as e:
            print(f"[ERROR] Package search failed: {str(e)}")

        # 4. List organizations
        print("\n4. LISTING ORGANIZATIONS")
        print("-" * 40)
        try:
            orgs = await client._make_request("GET", "organization_list")
            if orgs:
                print(f"Found {len(orgs)} organizations:")
                for i, org in enumerate(orgs[:10], 1):
                    print(f"  {i}. {org}")
            else:
                print("No organizations found")
        except Exception as e:
            print(f"[ERROR] Organization list failed: {str(e)}")

        # 5. List tags
        print("\n5. LISTING POPULAR TAGS")
        print("-" * 40)
        try:
            tags = await client._make_request("GET", "tag_list")
            if tags:
                print(f"Found {len(tags)} tags total")
                print("Sample tags:")
                for tag in tags[:15]:
                    print(f"  - {tag}")
            else:
                print("No tags found")
        except Exception as e:
            print(f"[ERROR] Tag list failed: {str(e)}")

        # 6. Show a specific package details (if any exist)
        print("\n6. PACKAGE DETAILS EXAMPLE")
        print("-" * 40)
        try:
            packages = await client._make_request("GET", "package_list?limit=1")
            if packages and len(packages) > 0:
                pkg_id = packages[0]
                print(f"Getting details for package: {pkg_id}")
                pkg_details = await client._make_request("GET", f"package_show?id={pkg_id}")

                print(f"\nPackage Details:")
                print(f"  Name: {pkg_details.get('name', 'N/A')}")
                print(f"  Title: {pkg_details.get('title', 'N/A')}")
                print(f"  Author: {pkg_details.get('author', 'N/A')}")
                print(f"  License: {pkg_details.get('license_title', 'N/A')}")
                print(f"  State: {pkg_details.get('state', 'N/A')}")
                print(f"  Type: {pkg_details.get('type', 'N/A')}")
                print(f"  Resources: {len(pkg_details.get('resources', []))}")

                if pkg_details.get('resources'):
                    print("\n  Resource details:")
                    for j, res in enumerate(pkg_details['resources'][:3], 1):
                        print(f"    {j}. {res.get('name', res.get('url', 'N/A'))}")
                        print(f"       Format: {res.get('format', 'N/A')}")
                        print(f"       Size: {res.get('size', 'N/A')} bytes")
            else:
                print("No packages available for detailed view")
        except Exception as e:
            print(f"[ERROR] Package details failed: {str(e)}")

        print("\n" + "=" * 60)
        print("Demo complete!")
        print("\nYou can run the MCP server with: python mcp_ckan_server.py")
        print("This will allow MCP clients to connect and use these CKAN tools.")

if __name__ == "__main__":
    print("CKAN MCP Server Demo")
    print("=" * 60)
    asyncio.run(demo_ckan_api())