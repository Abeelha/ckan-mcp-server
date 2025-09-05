
#!/usr/bin/env python3

import asyncio

import json
import logging
import os
import pprint
import ssl
import certifi
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, quote
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import time
import aiohttp
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from dotenv import load_dotenv


load_dotenv()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
# Configure logging
logging.basicConfig(level=logging.INFO,filename="mcp-ckan-server.log")
logger = logging.getLogger("mcp-ckan-server")
class MCPErrorType(Enum):
    INVALID_PARAMS = "invalid_parameters"
    CKAN_API_ERROR = "ckan_api_error"
    NETWORK_ERROR = "network_error"
    PERMISSION_DENIED = "permission_denied"
    DATA_NOT_FOUND = "data_not_found"

class StandardResponse:
    def __init__(self, success: bool, data: Optional[Any] = None, error: Optional[Dict] = None):
        self.success = success
        self.data = data
        self.error = error
        self.metadata = {
            "timestamp": datetime.utcnow().isoformat(),
            "execution_time_ms": 0,
            "api_version": "2.0.0"
        }

    def to_dict(self):
        result = {
            "success": self.success,
            "metadata": self.metadata
        }
        if self.data is not None:
            result["data"] = self.data
        if self.error:
            result["error"] = self.error
        return result

class CKANAPIClient:
    """CKAN API client for making HTTP requests"""

    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = None
        self.cache = {}
        self.cache_ttl = 300

    async def __aenter__(self):
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'MCP-CKAN-Server/1.0'
        }
        if self.api_key:
            headers['Authorization'] = self.api_key
        return headers

    def _get_cache_key(self, method: str, endpoint: str, data: Optional[Dict] = None) -> str:
        cache_data = f"{method}:{endpoint}:{json.dumps(data, sort_keys=True) if data else ''}"
        return hashlib.md5(cache_data.encode()).hexdigest()

    def _is_cache_valid(self, cache_entry: Dict) -> bool:
        if not cache_entry:
            return False
        cached_time = cache_entry.get("timestamp", 0)
        return (time.time() - cached_time) < self.cache_ttl

    async def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, use_cache: bool = True) -> Dict[str, Any]:
        cache_key = self._get_cache_key(method, endpoint, data)
        if use_cache and method == "GET":
            cache_entry = self.cache.get(cache_key)
            if cache_entry and self._is_cache_valid(cache_entry):
                logger.info(f"Cache hit for {endpoint}")
                return cache_entry["data"]

        url = urljoin(f"{self.base_url}/api/3/action/", endpoint)
        headers = self._get_headers()

        try:
            start_time = time.time()
            async with self.session.request(method, url, headers=headers, json=data) as response:
                result = await response.json()
                execution_time = int((time.time() - start_time) * 1000)

                if not result.get('success', False):
                    error_msg = result.get('error', {})
                    raise Exception(f"CKAN API Error: {error_msg}")

                result_data = result.get('result', {})

                if use_cache and method == "GET":
                    self.cache[cache_key] = {
                        "timestamp": time.time(),
                        "data": result_data
                    }

                return result_data

        except aiohttp.ClientError as e:
            raise Exception(f"HTTP Error: {str(e)}")
        except Exception as e:
            raise Exception(f"Request failed: {str(e)}")

    async def faceted_search(self, q: str = "*:*", facet_fields: List[str] = None,
                            filters: Dict = None, spatial_query: Dict = None,
                            date_range: Dict = None) -> Dict:
        params = {
            "q": q,
            "facet": "true",
            "facet.field": json.dumps(facet_fields) if facet_fields else '["tags", "organization", "res_format"]',
            "rows": 100
        }
        if filters:
            fq_parts = []
            for field, value in filters.items():
                fq_parts.append(f"{field}:{value}")
            params["fq"] = " AND ".join(fq_parts)

        if spatial_query:
            if "bbox" in spatial_query:
                bbox = spatial_query["bbox"]
                params["ext_bbox"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            elif "point" in spatial_query and "radius" in spatial_query:
                point = spatial_query["point"]
                radius = spatial_query["radius"]
                params["ext_spatial"] = f"{point[0]},{point[1]},{radius}"

        if date_range:
            start_date = date_range.get("start", "*")
            end_date = date_range.get("end", "*")
            date_field = date_range.get("field", "metadata_created")
            date_fq = f"{date_field}:[{start_date} TO {end_date}]"
            if "fq" in params:
                params["fq"] += f" AND {date_fq}"
            else:
                params["fq"] = date_fq

        query_string = "&".join([f"{k}={quote(str(v))}" for k, v in params.items()])
        return await self._make_request("GET", f"package_search?{query_string}")

    async def get_related_datasets(self, dataset_id: str, relation_type: str = "tags",
                                  max_results: int = 10) -> List[Dict]:


        source_dataset = await self._make_request("GET", f"package_show?id={dataset_id}")

        related_datasets = []

        if relation_type == "tags":

            tags = [tag["name"] for tag in source_dataset.get("tags", [])]
            if tags:
                tag_query = " OR ".join([f"tags:{tag}" for tag in tags])
                query_string = f"q=*:*&fq={quote(tag_query)}&rows={max_results}"
                results = await self._make_request("GET", f"package_search?{query_string}")
                related_datasets = [ds for ds in results.get("results", []) if ds["id"] != dataset_id]

        elif relation_type == "organization":

            org = source_dataset.get("organization", {}).get("id")
            if org:
                query_string = f"q=organization:{org}&rows={max_results}"
                results = await self._make_request("GET", f"package_search?{query_string}")
                related_datasets = [ds for ds in results.get("results", []) if ds["id"] != dataset_id]

        elif relation_type == "theme":

            groups = source_dataset.get("groups", [])
            if groups:
                group_query = " OR ".join([f"groups:{g['id']}" for g in groups])
                query_string = f"q=*:*&fq={quote(group_query)}&rows={max_results}"
                results = await self._make_request("GET", f"package_search?{query_string}")
                related_datasets = [ds for ds in results.get("results", []) if ds["id"] != dataset_id]

        return related_datasets[:max_results]

    async def check_data_quality(self, dataset_id: str, checks: List[str] = None,
                                sample_size: int = 100) -> Dict:

        if checks is None:
            checks = ["completeness", "format_validation", "schema_compliance"]

        dataset = await self._make_request("GET", f"package_show?id={dataset_id}")
        quality_report = {
            "dataset_id": dataset_id,
            "dataset_name": dataset.get("name"),
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {},
            "overall_score": 0
        }

        scores = []

        if "completeness" in checks:

            required_fields = ["title", "notes", "tags", "organization", "resources"]
            present_fields = sum(1 for field in required_fields if dataset.get(field))
            completeness_score = (present_fields / len(required_fields)) * 100
            quality_report["checks"]["completeness"] = {
                "score": completeness_score,
                "required_fields": required_fields,
                "present_fields": present_fields
            }
            scores.append(completeness_score)

        if "format_validation" in checks:

            resources = dataset.get("resources", [])
            valid_formats = ["CSV", "JSON", "XML", "XLS", "XLSX", "PDF", "TXT"]
            format_scores = []
            for resource in resources:
                res_format = resource.get("format", "").upper()
                is_valid = res_format in valid_formats
                has_url = bool(resource.get("url"))
                format_scores.append(100 if (is_valid and has_url) else 0)

            format_score = sum(format_scores) / len(format_scores) if format_scores else 0
            quality_report["checks"]["format_validation"] = {
                "score": format_score,
                "resource_count": len(resources),
                "valid_formats": valid_formats
            }
            scores.append(format_score)

        if "schema_compliance" in checks:

            has_license = bool(dataset.get("license_id"))
            has_author = bool(dataset.get("author") or dataset.get("author_email"))
            has_maintainer = bool(dataset.get("maintainer") or dataset.get("maintainer_email"))
            has_temporal = bool(dataset.get("temporal_coverage_from") or dataset.get("temporal_coverage_to"))
            has_spatial = bool(dataset.get("spatial"))

            compliance_items = [has_license, has_author, has_maintainer, has_temporal, has_spatial]
            compliance_score = (sum(compliance_items) / len(compliance_items)) * 100

            quality_report["checks"]["schema_compliance"] = {
                "score": compliance_score,
                "has_license": has_license,
                "has_author": has_author,
                "has_maintainer": has_maintainer,
                "has_temporal_coverage": has_temporal,
                "has_spatial_coverage": has_spatial
            }
            scores.append(compliance_score)

        quality_report["overall_score"] = sum(scores) / len(scores) if scores else 0
        return quality_report

    async def get_dataset_analytics(self, dataset_id: str = "all", time_range: Dict = None,
                                   metrics: List[str] = None) -> Dict:

        if metrics is None:
            metrics = ["views", "downloads", "api_calls", "resource_count"]

        analytics = {
            "dataset_id": dataset_id,
            "time_range": time_range or {"start": "30_days_ago", "end": "now"},
            "metrics": {}
        }

        if dataset_id == "all":

            site_stats = await self._make_request("GET", "status_show")
            package_list = await self._make_request("GET", "package_list")

            analytics["metrics"]["total_datasets"] = len(package_list)
            analytics["metrics"]["site_status"] = site_stats

            recent_query = "q=*:*&sort=metadata_created desc&rows=10"
            recent = await self._make_request("GET", f"package_search?{recent_query}")
            analytics["metrics"]["recent_datasets"] = recent.get("count", 0)

            popular_query = "q=*:*&sort=num_resources desc&rows=10"
            popular = await self._make_request("GET", f"package_search?{popular_query}")
            analytics["metrics"]["popular_datasets"] = [
                {"id": ds["id"], "name": ds["name"], "resources": ds.get("num_resources", 0)}
                for ds in popular.get("results", [])[:5]
            ]
        else:

            dataset = await self._make_request("GET", f"package_show?id={dataset_id}")

            if "resource_count" in metrics:
                analytics["metrics"]["resource_count"] = len(dataset.get("resources", []))

            if "views" in metrics:

                analytics["metrics"]["views"] = dataset.get("tracking_summary", {}).get("total", 0)

            if "downloads" in metrics:

                analytics["metrics"]["downloads"] = dataset.get("tracking_summary", {}).get("recent", 0)

            analytics["metrics"]["created"] = dataset.get("metadata_created")
            analytics["metrics"]["modified"] = dataset.get("metadata_modified")
            analytics["metrics"]["tags_count"] = len(dataset.get("tags", []))
            analytics["metrics"]["organization"] = dataset.get("organization", {}).get("name")

        return analytics

    async def preview_resource(self, resource_id: str, preview_rows: int = 10,
                              generate_stats: bool = True) -> Dict:

        resource = await self._make_request("GET", f"resource_show?id={resource_id}")

        preview = {
            "resource_id": resource_id,
            "resource_name": resource.get("name"),
            "format": resource.get("format"),
            "url": resource.get("url"),
            "size": resource.get("size"),
            "created": resource.get("created"),
            "last_modified": resource.get("last_modified")
        }

        try:
            datastore_info = await self._make_request("GET", f"datastore_search?resource_id={resource_id}&limit={preview_rows}")
            preview["preview_data"] = datastore_info.get("records", [])
            preview["total_records"] = datastore_info.get("total", 0)

            if generate_stats and datastore_info.get("fields"):

                fields_stats = []
                for field in datastore_info.get("fields", []):
                    field_stat = {
                        "id": field.get("id"),
                        "type": field.get("type"),
                        "info": field.get("info", {})
                    }
                    fields_stats.append(field_stat)
                preview["field_statistics"] = fields_stats
        except:
            preview["preview_data"] = None
            preview["note"] = "DataStore not available for this resource"

        return preview

    async def export_metadata(self, dataset_ids: List[str], export_format: str = "dcat",
                            include_resources: bool = True) -> Dict:

        exported_data = {
            "export_format": export_format,
            "timestamp": datetime.utcnow().isoformat(),
            "datasets": []
        }

        for dataset_id in dataset_ids:
            dataset = await self._make_request("GET", f"package_show?id={dataset_id}")

            if export_format == "dcat":

                dcat_dataset = {
                    "@type": "dcat:Dataset",
                    "dct:identifier": dataset.get("id"),
                    "dct:title": dataset.get("title"),
                    "dct:description": dataset.get("notes"),
                    "dcat:keyword": [tag["name"] for tag in dataset.get("tags", [])],
                    "dct:issued": dataset.get("metadata_created"),
                    "dct:modified": dataset.get("metadata_modified"),
                    "dct:publisher": {
                        "@type": "foaf:Organization",
                        "foaf:name": dataset.get("organization", {}).get("name")
                    }
                }

                if include_resources:
                    dcat_dataset["dcat:distribution"] = []
                    for resource in dataset.get("resources", []):
                        distribution = {
                            "@type": "dcat:Distribution",
                            "dct:identifier": resource.get("id"),
                            "dct:title": resource.get("name"),
                            "dcat:accessURL": resource.get("url"),
                            "dct:format": resource.get("format"),
                            "dcat:byteSize": resource.get("size")
                        }
                        dcat_dataset["dcat:distribution"].append(distribution)

                exported_data["datasets"].append(dcat_dataset)

            elif export_format == "schema_org":

                schema_dataset = {
                    "@context": "https://schema.org/",
                    "@type": "Dataset",
                    "name": dataset.get("title"),
                    "description": dataset.get("notes"),
                    "identifier": dataset.get("id"),
                    "keywords": [tag["name"] for tag in dataset.get("tags", [])],
                    "dateCreated": dataset.get("metadata_created"),
                    "dateModified": dataset.get("metadata_modified"),
                    "publisher": {
                        "@type": "Organization",
                        "name": dataset.get("organization", {}).get("name")
                    }
                }

                if include_resources:
                    schema_dataset["distribution"] = []
                    for resource in dataset.get("resources", []):
                        distribution = {
                            "@type": "DataDownload",
                            "name": resource.get("name"),
                            "contentUrl": resource.get("url"),
                            "encodingFormat": resource.get("format"),
                            "contentSize": resource.get("size")
                        }
                        schema_dataset["distribution"].append(distribution)

                exported_data["datasets"].append(schema_dataset)

            elif export_format == "ckan_native":

                exported_data["datasets"].append(dataset)

        return exported_data

ckan_client = None

# Initialize MCP server
server = Server("ckan-mcp-server")

@server.list_tools()
async def handle_list_tools() -> List[types.Tool]:
    """List available CKAN API tools"""
    return [
        types.Tool(
            name="ckan_package_list",
            description="Get list of all packages (datasets) in CKAN (unsorted)",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of packages to return",
                        "default": 100
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Offset for pagination",
                        "default": 0
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_package_show",
            description="Get details of a specific package/dataset (like dates)",
            inputSchema={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Package ID or name"
                    }
                },
                "required": ["id"]
            }
        ),
        types.Tool(
            name="ckan_package_search",
            description="Search for packages using queries",
            inputSchema={
                "type": "object",
                "properties": {
                    "q": {
                        "type": "string",
                        "description": "Search query",
                        "default": "*:*"
                    },
                    "fq": {
                        "type": "string",
                        "description": "Filter query"
                    },
                    "sort": {
                        "type": "string",
                        "description": "Sort field and direction (e.g., 'score desc')"
                    },
                    "rows": {
                        "type": "integer",
                        "description": "Number of results to return",
                        "default": 10
                    },
                    "start": {
                        "type": "integer",
                        "description": "Offset for pagination",
                        "default": 0
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_organization_list",
            description="Get list of all organizations",
            inputSchema={
                "type": "object",
                "properties": {
                    "all_fields": {
                        "type": "boolean",
                        "description": "Include all organization fields",
                        "default": False
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_organization_show",
            description="Get details of a specific organization",
            inputSchema={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Organization ID or name"
                    },
                    "include_datasets": {
                        "type": "boolean",
                        "description": "Include organization's datasets",
                        "default": False
                    }
                },
                "required": ["id"]
            }
        ),
        types.Tool(
            name="ckan_group_list",
            description="Get list of all groups",
            inputSchema={
                "type": "object",
                "properties": {
                    "all_fields": {
                        "type": "boolean",
                        "description": "Include all group fields",
                        "default": False
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_tag_list",
            description="Get list of all tags",
            inputSchema={
                "type": "object",
                "properties": {
                    "vocabulary_id": {
                        "type": "string",
                        "description": "Vocabulary ID to filter tags"
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_resource_show",
            description="Get details of a specific resource",
            inputSchema={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Resource ID"
                    }
                },
                "required": ["id"]
            }
        ),
        types.Tool(
            name="ckan_site_read",
            description="Get site information and statistics",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        types.Tool(
            name="ckan_status_show",
            description="Get CKAN site status and version information",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),

        types.Tool(
            name="ckan_faceted_search",
            description="Advanced search with faceting for refined data discovery",
            inputSchema={
                "type": "object",
                "properties": {
                    "q": {"type": "string", "description": "Search query", "default": "*:*"},
                    "facet_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fields to facet on (tags, organizations, formats)"
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filter conditions as field:value pairs"
                    },
                    "spatial_query": {
                        "type": "object",
                        "description": "Geographic search with bbox or point+radius"
                    },
                    "date_range": {
                        "type": "object",
                        "description": "Temporal filtering with start/end dates"
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_related_datasets",
            description="Find datasets related to a given dataset",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string", "description": "Source dataset ID"},
                    "relation_type": {
                        "type": "string",
                        "enum": ["tags", "theme", "organization"],
                        "description": "Type of relationship",
                        "default": "tags"
                    },
                    "max_results": {"type": "integer", "description": "Maximum results", "default": 10}
                },
                "required": ["dataset_id"]
            }
        ),
        types.Tool(
            name="ckan_data_quality_check",
            description="Analyze data quality metrics for datasets",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string", "description": "Dataset to analyze"},
                    "checks": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["completeness", "format_validation", "schema_compliance"]
                        },
                        "description": "Quality checks to perform"
                    },
                    "sample_size": {"type": "integer", "description": "Records to sample", "default": 100}
                },
                "required": ["dataset_id"]
            }
        ),
        types.Tool(
            name="ckan_dataset_analytics",
            description="Generate analytics about dataset usage and engagement",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string", "description": "Dataset ID or 'all'", "default": "all"},
                    "time_range": {
                        "type": "object",
                        "description": "Analytics time period"
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Metrics to calculate"
                    }
                }
            }
        ),
        types.Tool(
            name="ckan_resource_preview",
            description="Generate preview and summary statistics for resources",
            inputSchema={
                "type": "object",
                "properties": {
                    "resource_id": {"type": "string", "description": "Resource ID"},
                    "preview_rows": {"type": "integer", "description": "Rows to preview", "default": 10},
                    "generate_stats": {"type": "boolean", "description": "Generate statistics", "default": True}
                },
                "required": ["resource_id"]
            }
        ),
        types.Tool(
            name="ckan_metadata_exporter",
            description="Export metadata in various standards (DCAT, Schema.org)",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Dataset IDs to export"
                    },
                    "export_format": {
                        "type": "string",
                        "enum": ["dcat", "schema_org", "ckan_native"],
                        "description": "Export format",
                        "default": "dcat"
                    },
                    "include_resources": {"type": "boolean", "description": "Include resources", "default": True}
                },
                "required": ["dataset_ids"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[types.TextContent]:
    """Handle tool calls to CKAN API"""
    if not ckan_client:
        error_response = StandardResponse(
            success=False,
            error={
                "type": MCPErrorType.CKAN_API_ERROR.value,
                "message": "CKAN client not initialized. Please set CKAN_URL environment variable."
            }
        )
        return [types.TextContent(type="text", text=json.dumps(error_response.to_dict(), indent=2))]

    start_time = time.time()

    try:
        if name == "ckan_package_list":
            limit = arguments.get("limit", 100) if arguments else 100
            offset = arguments.get("offset", 0) if arguments else 0
            result = await ckan_client._make_request("GET", f"package_list?limit={limit}&offset={offset}")

        elif name == "ckan_package_show":
            package_id = arguments["id"]
            result = await ckan_client._make_request("GET", f"package_show?id={package_id}")

        elif name == "ckan_package_search":
            params = arguments or {}
            query_params = []
            for key, value in params.items():
                if value is not None:
                    query_params.append(f"{key}={value}")
            query_string = "&".join(query_params)
            result = await ckan_client._make_request("GET", f"package_search?{query_string}")

        elif name == "ckan_organization_list":
            all_fields = arguments.get("all_fields", False) if arguments else False
            result = await ckan_client._make_request("GET", f"organization_list?all_fields={all_fields}")

        elif name == "ckan_organization_show":
            org_id = arguments["id"]
            include_datasets = arguments.get("include_datasets", False)
            result = await ckan_client._make_request("GET", f"organization_show?id={org_id}&include_datasets={include_datasets}")

        elif name == "ckan_group_list":
            all_fields = arguments.get("all_fields", False) if arguments else False
            result = await ckan_client._make_request("GET", f"group_list?all_fields={all_fields}")

        elif name == "ckan_tag_list":
            params = arguments or {}
            query_params = []
            for key, value in params.items():
                if value is not None:
                    query_params.append(f"{key}={value}")
            query_string = "&".join(query_params)
            endpoint = f"tag_list?{query_string}" if query_string else "tag_list"
            result = await ckan_client._make_request("GET", endpoint)

        elif name == "ckan_resource_show":
            resource_id = arguments["id"]
            result = await ckan_client._make_request("GET", f"resource_show?id={resource_id}")

        elif name == "ckan_site_read":
            result = await ckan_client._make_request("GET", "site_read")

        elif name == "ckan_status_show":
            result = await ckan_client._make_request("GET", "status_show")

        elif name == "ckan_faceted_search":
            result = await ckan_client.faceted_search(
                q=arguments.get("q", "*:*"),
                facet_fields=arguments.get("facet_fields"),
                filters=arguments.get("filters"),
                spatial_query=arguments.get("spatial_query"),
                date_range=arguments.get("date_range")
            )

        elif name == "ckan_related_datasets":
            result = await ckan_client.get_related_datasets(
                dataset_id=arguments["dataset_id"],
                relation_type=arguments.get("relation_type", "tags"),
                max_results=arguments.get("max_results", 10)
            )

        elif name == "ckan_data_quality_check":
            result = await ckan_client.check_data_quality(
                dataset_id=arguments["dataset_id"],
                checks=arguments.get("checks"),
                sample_size=arguments.get("sample_size", 100)
            )

        elif name == "ckan_dataset_analytics":
            result = await ckan_client.get_dataset_analytics(
                dataset_id=arguments.get("dataset_id", "all"),
                time_range=arguments.get("time_range"),
                metrics=arguments.get("metrics")
            )

        elif name == "ckan_resource_preview":
            result = await ckan_client.preview_resource(
                resource_id=arguments["resource_id"],
                preview_rows=arguments.get("preview_rows", 10),
                generate_stats=arguments.get("generate_stats", True)
            )

        elif name == "ckan_metadata_exporter":
            result = await ckan_client.export_metadata(
                dataset_ids=arguments["dataset_ids"],
                export_format=arguments.get("export_format", "dcat"),
                include_resources=arguments.get("include_resources", True)
            )

        else:
            raise Exception(f"Unknown tool: {name}")

        response = StandardResponse(success=True, data=result)
        response.metadata["execution_time_ms"] = int((time.time() - start_time) * 1000)

        return [
            types.TextContent(
                type="text",
                text=json.dumps(response.to_dict(), indent=2, ensure_ascii=False)
            )
        ]

    except Exception as e:
        logger.error(f"Error calling tool {name}: {str(e)}")

        error_type = MCPErrorType.CKAN_API_ERROR
        if "not found" in str(e).lower():
            error_type = MCPErrorType.DATA_NOT_FOUND
        elif "permission" in str(e).lower() or "unauthorized" in str(e).lower():
            error_type = MCPErrorType.PERMISSION_DENIED
        elif "network" in str(e).lower() or "connection" in str(e).lower():
            error_type = MCPErrorType.NETWORK_ERROR
        elif "invalid" in str(e).lower() or "parameter" in str(e).lower():
            error_type = MCPErrorType.INVALID_PARAMS

        error_response = StandardResponse(
            success=False,
            error={
                "type": error_type.value,
                "message": str(e),
                "tool": name,
                "arguments": arguments
            }
        )
        error_response.metadata["execution_time_ms"] = int((time.time() - start_time) * 1000)

        return [
            types.TextContent(
                type="text",
                text=json.dumps(error_response.to_dict(), indent=2)
            )
        ]

@server.list_resources()
async def handle_list_resources() -> List[types.Resource]:
    """List available CKAN resources"""
    return [
        types.Resource(
            uri="ckan://api/docs",
            name="CKAN API Documentation",
            description="Official CKAN API documentation and endpoints",
            mimeType="text/plain"
        ),
        types.Resource(
            uri="ckan://config",
            name="CKAN Server Configuration",
            description="Current CKAN server configuration and connection details",
            mimeType="application/json"
        ),
        types.Resource(
            uri="ckan://enhanced/features",
            name="Enhanced Features Documentation",
            description="Documentation for enhanced MCP server features",
            mimeType="text/plain"
        )
    ]

@server.read_resource()
async def handle_read_resource(uri: str) -> str:
    """Read CKAN resources"""
    if uri == "ckan://api/docs":
        return """
CKAN API Documentation Summary

Base URL: Configure via CKAN_URL environment variable
API Version: 3

Key Endpoints:
- package_list: Get all packages/datasets
- package_show: Get package details
- package_search: Search packages
- organization_list: Get all organizations
- organization_show: Get organization details
- group_list: Get all groups
- tag_list: Get all tags
- resource_show: Get resource details
- site_read: Get site information
- status_show: Get site status

Authentication: Set CKAN_API_KEY environment variable for write operations

Full documentation: https://docs.ckan.org/en/latest/api/
        """
    elif uri == "ckan://config":
        config = {
            "base_url": ckan_client.base_url if ckan_client else "Not configured",
            "api_key_configured": bool(ckan_client and ckan_client.api_key),
            "session_active": bool(ckan_client and ckan_client.session),
            "cache_enabled": True,
            "cache_ttl": ckan_client.cache_ttl if ckan_client else 300,
            "enhanced_features": [
                "faceted_search", "related_datasets", "data_quality_check",
                "dataset_analytics", "resource_preview", "metadata_exporter"
            ]
        }
        return json.dumps(config, indent=2)
    elif uri == "ckan://enhanced/features":
        return
    else:
        raise Exception(f"Unknown resource: {uri}")

async def main():
    """Main server function"""
    import os

    # Initialize CKAN client
    ckan_url = os.getenv("CKAN_URL")
    if not ckan_url:
        logger.error("CKAN_URL environment variable not set")
        raise Exception("CKAN_URL environment variable is required")

    ckan_api_key = os.getenv("CKAN_API_KEY")

    global ckan_client
    ckan_client = CKANAPIClient(ckan_url, ckan_api_key)

    # Start the CKAN client session
    await ckan_client.__aenter__()

    try:
        # Run the MCP server
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="ckan-mcp-server",
                    server_version="1.0.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    finally:
        # Clean up CKAN client
        await ckan_client.__aexit__(None, None, None)

if __name__ == "__main__":
    asyncio.run(main())
