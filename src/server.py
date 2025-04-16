#!/usr/bin/env python3
"""
Glassnode API MCP Server

A Model Context Protocol server that provides resources and tools
for interacting with the Glassnode API.
"""
import os
import json
from typing import Dict, List, Optional
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP, Context

from glassnode_api import GlassnodeAPIClient

# Load environment variables from .env file
load_dotenv()


@dataclass
class AppContext:
    """Application context for the MCP server."""
    api_client: GlassnodeAPIClient


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
        raise ValueError("GLASSNODE_API_KEY environment variable not set. Please set it before running the server.")
    
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
)


# Resources for metadata
# @mcp.resource("assets://list")
@mcp.tool()
async def get_assets_list(ctx: Context) -> str:
    """
    Get a list of all assets supported by Glassnode API.
    
    Returns:
        str: JSON string containing the list of assets
    """
    api_client = ctx.request_context.lifespan_context.api_client
    try:
        assets = await api_client.get_assets_list()
        return json.dumps(assets, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# @mcp.resource("metrics://list")
@mcp.tool()
async def get_metrics_list(ctx: Context) -> str:
    """
    Get a list of all available metrics.
    
    Returns:
        str: JSON string containing the list of metrics
    """
    api_client = ctx.request_context.lifespan_context.api_client
    try:
        metrics = await api_client.get_metrics_list()
        return json.dumps(metrics, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# @mcp.resource("metrics://{path}")
@mcp.tool()
async def get_metric_metadata(ctx: Context, path: str) -> str:
    """
    Get metadata for a specific metric.
    
    Args:
        path: The metric path (e.g., "market/price_usd_close")
    
    Returns:
        str: JSON string containing the metric metadata
    """
    api_client = ctx.request_context.lifespan_context.api_client
    try:
        metadata = await api_client.get_metric_metadata(path)
        return json.dumps(metadata, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# Tools for data retrieval
@mcp.tool()
async def fetch_metric(
    path: str, 
    asset: str, 
    since: Optional[int] = None, 
    until: Optional[int] = None,
    interval: Optional[str] = None,
    format: str = "json",
    ctx: Context = None,
    **kwargs
) -> Dict:
    """
    Fetch data for a specific metric.
    
    Args:
        path: The metric path (e.g., "/market/price_usd_close")
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
        data = await api_client.fetch_metric(
            path=path,
            asset=asset,
            since=since,
            until=until,
            interval=interval,
            format=format,
            **kwargs
        )
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@mcp.tool()
async def fetch_bulk_metric(
    path: str, 
    assets: Optional[List[str]] = None, 
    since: Optional[int] = None, 
    until: Optional[int] = None,
    interval: str = "24h",
    format: str = "json",
    ctx: Context = None,
    **kwargs
) -> Dict:
    """
    Fetch data for a metric using Glassnode's bulk endpoint.
    
    Args:
        path: The metric path (e.g., "/market/price_usd_close")
        assets: Optional list of asset symbols (e.g., ["BTC", "ETH"])
        since: Optional start date as Unix timestamp
        until: Optional end date as Unix timestamp
        interval: Resolution interval (defaults to "24h")
        format: Response format ('json' or 'csv')
        ctx: MCP context object
        **kwargs: Additional parameters to pass to the API
        
    Returns:
        Dict: The bulk metric data
    """
    if ctx is None:
        return {"error": "Context not available"}
    
    api_client = ctx.request_context.lifespan_context.api_client
    
    try:
        data = await api_client.fetch_bulk_metric(
            path=path,
            assets=assets,
            since=since,
            until=until,
            interval=interval,
            format=format,
            **kwargs
        )
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Run the server when executed directly
if __name__ == "__main__":
    # For development purposes
    mcp.run() 