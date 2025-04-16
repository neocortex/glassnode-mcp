"""
Glassnode API Client

A simple async wrapper for the Glassnode API.
"""
from typing import Dict, List, Optional, Any, Union
import httpx
import json
import time


class GlassnodeAPIClient:
    """
    Client for interacting with the Glassnode API.
    
    This class provides methods for fetching data from various
    Glassnode API endpoints including metadata and metrics.
    """
    
    BASE_URL = "https://api.glassnode.com/v1"
    
    # Maximum allowed timerange in days for bulk endpoints
    BULK_MAX_DAYS = {
        "10m": 10,
        "1h": 10,
        "24h": 31,
        "1w": 93,
        "1month": 93
    }
    
    def __init__(self, api_key: str):
        """
        Initialize the Glassnode API client.
        
        Args:
            api_key: API key for Glassnode API authentication
        """
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def _make_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Dict:
        """
        Make a request to the Glassnode API.
        
        Args:
            endpoint: API endpoint path
            params: Optional query parameters
            
        Returns:
            Dict: Response data as a dictionary
            
        Raises:
            httpx.HTTPStatusError: If the HTTP request returns an error status code
            ValueError: If the response is not valid JSON
        """
        if params is None:
            params = {}
        
        # Add API key to parameters
        params["api_key"] = self.api_key
        
        # Remove leading slash from endpoint if present
        endpoint = endpoint.lstrip("/")
        
        # Make request
        url = f"{self.BASE_URL}/{endpoint}"
        response = await self.client.get(url, params=params)
        
        # Raise exception for HTTP errors
        response.raise_for_status()
        
        # Parse JSON response
        try:
            return response.json()
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON response: {response.text}")
    
    # Metadata endpoints
    
    async def get_assets_list(self) -> List[Dict]:
        """
        Get a list of all supported assets.
        
        Returns:
            List[Dict]: List of asset metadata objects
        """
        return await self._make_request("metadata/assets")
    
    async def get_metrics_list(self) -> List[Dict]:
        """
        Get a list of all available metrics.
        
        Returns:
            List[Dict]: List of available metric objects
        """
        return await self._make_request("metadata/metrics")
    
    async def get_metric_metadata(self, path: str, asset: Optional[str] = None) -> Dict:
        """
        Get metadata for a specific metric.
        
        Args:
            path: The metric path (e.g., "market/price_usd_close")
            asset: Optional asset ID to retrieve asset-specific metadata
            
        Returns:
            Dict: Metric metadata
            
        Raises:
            httpx.HTTPStatusError: If the endpoint returns an error (e.g., invalid path)
            ValueError: If the response is not valid JSON
        """
        params = {"path": path}
        if asset:
            params["a"] = asset
        
        return await self._make_request("metadata/metric", params)
    
    # Data endpoints
    
    async def fetch_metric(
        self,
        path: str,
        asset: str,
        since: Optional[int] = None,
        until: Optional[int] = None,
        interval: Optional[str] = None,
        format: str = "json",
        **kwargs
    ) -> Union[List[Dict], str]:
        """
        Fetch data for a specific metric.
        
        Args:
            path: The metric path (e.g., "market/price_usd_close")
            asset: The asset symbol (e.g., "BTC")
            since: Optional start date as Unix timestamp
            until: Optional end date as Unix timestamp
            interval: Optional resolution interval 
            format: Response format ('json' or 'csv')
            **kwargs: Additional parameters to pass to the API
            
        Returns:
            Union[List[Dict], str]: Metric data as list of dictionaries or CSV string
        """
        params = {"a": asset, "f": format, **kwargs}
        
        if since is not None:
            params["s"] = since
        
        if until is not None:
            params["u"] = until
        
        if interval is not None:
            params["i"] = interval
        
        return await self._make_request(f"metrics{path}", params)
    
    async def fetch_bulk_metric(
        self,
        path: str,
        assets: Optional[List[str]] = None,
        since: Optional[int] = None,
        until: Optional[int] = None,
        interval: str = "24h",
        format: str = "json",
        **kwargs
    ) -> Dict:
        """
        Fetch data for a metric using Glassnode's bulk endpoint.
        
        The bulk endpoint allows fetching data for multiple parameter combinations 
        (like multiple assets) in a single API call.
        
        Args:
            path: The metric path without the "/bulk" suffix (e.g., "market/price_usd_close")
            assets: Optional list of asset symbols (e.g., ["BTC", "ETH"])
            since: Optional start date as Unix timestamp
            until: Optional end date as Unix timestamp
            interval: Optional resolution interval (defaults to "24h")
            format: Response format (only 'json' is currently supported by Glassnode)
            **kwargs: Additional parameters to pass to the API
            
        Returns:
            Dict: Bulk metric data in the format described in the Glassnode documentation
            
        Raises:
            ValueError: If the metric does not support bulk operations
            
        Note:
            The response format differs from regular metrics. See:
            https://docs.glassnode.com/basic-api/bulk-metrics
            
            Timerange constraints by interval:
            - 10m and 1h resolutions: 10 days
            - 24h resolution: 31 days
            - 1w and 1month resolutions: 93 days
        """
        # Check if the metric supports bulk operations
        metadata = await self.get_metric_metadata(path)
        if not metadata.get("bulk_supported", False):
            raise ValueError(f"Metric '{path}' does not support bulk operations")
        
        # Construct the bulk path
        bulk_path = f"{path}/bulk"
        
        params = {"i": interval, "f": format, **kwargs}
        
        # Add multiple asset parameters if provided
        if assets:
            params["a"] = assets
        
        # Apply timerange constraints based on interval
        until_ts = int(time.time()) if until is None else until
        
        # Determine max allowed timerange (in seconds) based on interval
        max_days = self.BULK_MAX_DAYS[interval]
        
        max_timerange = max_days * 24 * 60 * 60
        
        # Calculate appropriate since timestamp
        if since is None:
            # If no since is provided, use the maximum allowed timerange
            since_ts = until_ts - max_timerange
        else:
            # If since is provided, validate against constraints
            if (until_ts - since) > max_timerange:
                # If timerange exceeds limit, adjust since to respect the constraint
                since_ts = until_ts - max_timerange
            else:
                since_ts = since
        
        params["s"] = since_ts
        params["u"] = until_ts
        
        return await self._make_request(f"metrics{bulk_path}", params) 