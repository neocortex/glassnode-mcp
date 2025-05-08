#!/usr/bin/env python3
"""
Glassnode API MCP Server

A Model Context Protocol server that provides resources and tools
for interacting with the Glassnode API.
"""
import os
import sys
import json
import asyncio
from typing import Dict, List, Optional
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP, Context

# Updated import due to src-layout in dependency
from glassnode_api.glassnode_client import GlassnodeAPIClient

# Load environment variables from .env file
load_dotenv()


@dataclass
class AppContext:
    """Application context for the MCP server."""

    api_client: GlassnodeAPIClient
    valid_metrics: Dict[str, Dict] = None


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """
    Manages the application lifecycle.
    
    Args:
        server: The FastMCP server instance
        
    Yields:
        AppContext: The application context with initialized resources
    """
    # Load API key from environment variables
    api_key = os.environ.get("GLASSNODE_API_KEY", "")
    if not api_key:
        raise ValueError(
            "GLASSNODE_API_KEY environment variable not set. Please set it before running the server."
        )

    # Initialize Glassnode API client
    api_client = GlassnodeAPIClient(api_key)

    try:
        yield AppContext(api_client=api_client)
    finally:
        # No cleanup needed for the client
        pass


# Create MCP server
mcp = FastMCP(
    "Glassnode MCP",
    lifespan=app_lifespan,
    host = os.getenv("HOST", "0.0.0.0"),
    port = int(os.getenv("PORT", "8050"))
)


# Resources for metadata
# @mcp.resource("assets://list")
@mcp.tool()
async def get_assets_list() -> str:
    """
    Get a list of all cryptocurrency assets supported by Glassnode API.
    
    This resource returns a JSON object containing all available assets
    and their metadata. Use this when you need to:
    
    - Verify if a specific asset is supported
    - Get the correct asset symbol to use in data fetching tools
    - Display available assets to users
    
    Returns:
        str: JSON string containing the list of assets with their metadata
    """
    try:
        assets_file = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), "assets.json"))
        if os.path.exists(assets_file):
            with open(assets_file, "r") as f:
                assets = json.load(f)
            return json.dumps(assets, indent=2)
        else:
            return json.dumps({"error": f"File {assets_file} not found"})
    except Exception as e:
        return json.dumps({"error": str(e)})


# @mcp.resource("metrics://list")
@mcp.tool()
async def get_metrics_list() -> str:
    """
    Get a list of all available metrics and their paths from Glassnode API.
    
    IMPORTANT: This resource should be used to retrieve the correct metric paths that are required
    when calling the data fetching tools (fetch_metric and fetch_bulk_metric). The paths returned
    by this resource are the exact format needed as the 'path' parameter for those tools.
    
    Examples of metric paths you might find:
    - /market/price_usd_close
    - /market/price_drawdown_relative
    - /blockchain/utxo_created_count
    
    Use this resource when you need to:
    - Find the correct path for a specific metric
    - Discover available metrics and their categories
    - Validate if a specific metric exists before attempting to fetch data
    - Display available metrics to users
    
    Returns:
        str: JSON string containing the list of all available metrics with their paths
    """
    try:
        metrics_file = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), "metrics.json"))
        if os.path.exists(metrics_file):
            with open(metrics_file, "r") as f:
                metrics = json.load(f)
            return json.dumps(metrics, indent=2)
        else:
            return json.dumps({"error": f"File {metrics_file} not found"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_asset_metrics(asset: str) -> str:
    """
    Get a list of available metrics for a specific asset.
    
    This resource returns a JSON object containing all metrics that are 
    available for the specified asset. Use this when you need to:
    
    - Find which metrics are supported for a specific asset
    - Filter available metrics by asset before making data requests
    - Check asset-specific metric availability
    
    Args:
        asset: The asset symbol (e.g., "BTC")
    
    Returns:
        str: JSON string containing the list of metrics available for the specified asset
    """
    try:
        metrics_file = os.path.abspath(
            os.path.join(os.path.dirname(sys.argv[0]), "metrics_per_asset.json")
        )
        if os.path.exists(metrics_file):
            with open(metrics_file, "r") as f:
                metrics_per_asset = json.load(f)

            if asset in metrics_per_asset:
                return json.dumps(metrics_per_asset[asset], indent=2)
            else:
                return json.dumps({"error": f"Asset '{asset}' not found in metrics_per_asset.json"})
        else:
            return json.dumps({"error": f"File {metrics_file} not found"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_metric_metadata(ctx: Context, metric_path: str) -> str:
    """
    Get metadata for a specific metric.
    
    IMPORTANT - First call the resource get_metrics_list() to get all available metrics. Verify that
    '{metric_path}' is a valid metric path. If not, find the closest matching metric path from the list.

    Args:
        path: The metric path (e.g., "/market/price_usd_close")
    
    Returns:
        str: JSON string containing the metric metadata
    """
    api_client = ctx.request_context.lifespan_context.api_client
    try:
        # Run synchronous call in a separate thread
        metadata = await asyncio.to_thread(api_client.get_metric_metadata, metric_path)
        return json.dumps(metadata, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# Tools for data retrieval
@mcp.tool()
async def fetch_metric(
    metric_path: str,
    asset: str,
    since: Optional[int] = None,
    until: Optional[int] = None,
    interval: Optional[str] = "24h",
    format: Optional[str] = "json",
    limit: Optional[int] = None,
    ctx: Context = None,
    **kwargs,
) -> Dict:
    """
    Fetch data for a specific metric.
    IMPORTANT - First call the resource get_metrics_list() to get all available metrics. Verify that
    '{metric_path}' is a valid metric path. If not, find the closest matching metric path from the list.
    
    Args:
        path: The metric path (e.g., "/market/price_usd_close"). To get the list of available metrics
            and correct path, call `get_metrics_list()`.
        asset: The asset symbol (e.g., "BTC")
        since: Optional start date as Unix timestamp
        until: Optional end date as Unix timestamp
        interval: Optional resolution interval
        format: Response format ('json' or 'csv')
        ctx: MCP context object
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Dict: The metric data as a dictionary
    """
    if ctx is None:
        return {"error": "Context not available"}

    api_client = ctx.request_context.lifespan_context.api_client

    try:
        # Run synchronous call in a separate thread
        data = await asyncio.to_thread(
            api_client.fetch_metric,
            path=metric_path,
            asset=asset,
            since=since,
            until=until,
            interval=interval,
            format=format,  # Request JSON/CSV from API
            limit=limit,
            return_format="raw",  # Ensure client returns raw dict/str
            **kwargs,
        )
        # Server expects dict, Glassnode client returns list[dict] for JSON or str for CSV
        # We'll wrap the data in a standard response format.
        # If format='csv', data will be a string; if 'json', it's list[dict]
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@mcp.tool()
async def fetch_bulk_metric(
    metric_path: str,
    assets: Optional[List[str]] = None,
    since: Optional[int] = None,
    until: Optional[int] = None,
    interval: str = "24h",
    limit: Optional[int] = None,
    ctx: Context = None,
    **kwargs,
) -> Dict:
    """
    Fetch data for a metric using Glassnode's bulk endpoint.
    IMPORTANT - First call the resource get_metrics_list() to get all available metrics. Verify that
    '{metric_path}' is a valid metric path. If not, find the closest matching metric path from the list.
    Args:
        path: The metric path (e.g., "/market/price_usd_close"). To get the list of available metrics
            and correct path, call `get_metrics_list()`.
        assets: Optional list of asset symbols (e.g., ["BTC", "ETH"])
        since: Optional start date as Unix timestamp
        until: Optional end date as Unix timestamp
        interval: Resolution interval (defaults to "24h")
        ctx: MCP context object
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Dict: The bulk metric data
    """
    if ctx is None:
        return {"error": "Context not available"}

    api_client = ctx.request_context.lifespan_context.api_client

    try:
        # Run synchronous call in a separate thread
        data = await asyncio.to_thread(
            api_client.fetch_bulk_metric,
            path=metric_path,
            assets=assets,
            since=since,
            until=until,
            interval=interval,
            limit=limit,
            return_format="raw",  # Ensure client returns the raw dict
            **kwargs,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}


async def main():
    """
    Main entry point for the MCP server.
    Determines which transport to use (stdio or SSE) based on environment variables.
    """
    # Simple transport selection based on environment variable
    transport = os.getenv("TRANSPORT", "sse")
    
    if transport == 'sse':      
        # Run the MCP server with SSE transport
        await mcp.run_sse_async()
    else:
        # Run the MCP server with stdio transport
        await mcp.run_stdio_async()


# Run the server when executed directly
if __name__ == "__main__":
    # For development purposes
    # mcp.run()
    asyncio.run(main())