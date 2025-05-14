# Glassnode API MCP Server

_Crypto on-chain metrics and market intelligence for AI agents_ 

A Model Context Protocol (MCP) server providing access to [Glassnode's](https://glassnode.com/) API. The server enables AI agents to access on-chain metrics, market data, and analytics.

For detailed information about the API and available data:
* [Glassnode API Documentation](https://docs.glassnode.com/basic-api/endpoints)
* [Glassnode Metric Catalog](https://docs.glassnode.com/data/metric-catalog)
* [Supported Assets](https://docs.glassnode.com/data/supported-assets)

## Features

* Asset and metrics discovery
* Metric metadata retrieval
* Single and bulk data retrieval

## Tools

The server provides six tools for accessing Glassnode data:

1. **`get_assets_list`**: Get a list of all cryptocurrencies and tokens supported by Glassnode
2. **`get_metrics_list`**: Get a catalog of all available metrics and their paths
3. **`get_asset_metrics`**: Get metrics available for a specific asset
4. **`get_metric_metadata`**: Get detailed information about a specific metric's structure
5. **`fetch_metric`**: Fetch data for a specific metric, asset, and time range
6. **`fetch_bulk_metric`**: Fetch data for a metric across multiple assets

## Prerequisites

* Python 3.11+
* A Glassnode API key ([available here](https://docs.glassnode.com/basic-api/api-key))

## Installation & Running

### Using uv (recommended)

1. Clone this repository:  
```
git clone https://github.com/neocortex/glassnode-mcp.git
cd glassnode-mcp
```

2. Install with uv:  
```
uv pip install -e .
```

3. Create a `.env` file with your Glassnode API key:
```
GLASSNODE_API_KEY=your_api_key_here
TRANSPORT=stdio  # or sse for server mode
HOST=0.0.0.0     # only needed for sse
PORT=8050        # only needed for sse
```

4. Run the server:
```
uv run --directory src server.py
```

### Using venv (alternative)

```
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
python -m src.server
```

## Integration with MCP Clients

### Stdio Integration

Add this server to your MCP configuration for Claude Desktop, Cursor, Windsurf, or any other MCP client:

```json
{
  "mcpServers": {
    "glassnode": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/glassnode-mcp/src",
        "server.py"
      ],
      "env": {
        "GLASSNODE_API_KEY": "your_api_key_here",
        "TRANSPORT": "stdio"
      }
    }
  }
}
```

> **Note for Claude Desktop**: You may need to provide the absolute path to the uv executable (e.g., `/path/to/your/bin/uv`) instead of just `"uv"`.

### SSE Integration

For SSE-based clients, first update your `.env` file with the following values:

```
GLASSNODE_API_KEY=your_api_key_here
TRANSPORT=sse
HOST=0.0.0.0
PORT=8050
```

Then start the server:

```
uv run --env-file .env src/server.py
```

Configure your MCP client:

```json
{
  "mcpServers": {
    "glassnode": {
      "transport": "sse",
      "url": "http://localhost:8050/sse"
    }
  }
}
```

> **Note for Windsurf users**: Use `serverUrl` instead of `url` in your configuration.
