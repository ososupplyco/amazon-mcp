"""
Minimal Python MCP server that wraps Amazon Selling Partner API (SP-API)
for inventory-report workflows, plus AWD inventory summaries.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import httpx
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from dotenv import load_dotenv
from fastmcp import FastMCP


load_dotenv()


@dataclass(frozen=True)
class Settings:
    lwa_client_id: str
    lwa_client_secret: str
    lwa_refresh_token: str
    aws_access_key_id: str
    aws_secret_access_key: str
    spapi_endpoint: str
    spapi_aws_region: str
    default_marketplace_id: str
    mcp_bearer_token: str

    @classmethod
    def from_env(cls) -> "Settings":
        missing = []

        def get_env(name: str, default: Optional[str] = None) -> str:
            value = os.getenv(name, default)
            if not value:
                missing.append(name)
                return ""
            return value

        settings = cls(
            lwa_client_id=get_env("AMAZON_LWA_CLIENT_ID"),
            lwa_client_secret=get_env("AMAZON_LWA_CLIENT_SECRET"),
            lwa_refresh_token=get_env("AMAZON_LWA_REFRESH_TOKEN"),
            aws_access_key_id=get_env("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=get_env("AWS_SECRET_ACCESS_KEY"),
            spapi_endpoint=get_env("SPAPI_ENDPOINT", "https://sellingpartnerapi-na.amazon.com"),
            spapi_aws_region=get_env("SPAPI_AWS_REGION", "us-east-1"),
            default_marketplace_id=get_env("DEFAULT_MARKETPLACE_ID", "ATVPDKIKX0DER"),
            mcp_bearer_token=get_env("MCP_BEARER_TOKEN"),
        )

        if missing:
            raise RuntimeError(
                "Missing required environment variables: " + ", ".join(sorted(set(missing)))
            )
        return settings


SETTINGS = Settings.from_env()

mcp = FastMCP(name="Amazon Inventory MCP")


class SPAPIError(RuntimeError):
    pass


class AmazonSPAPIClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._http = httpx.Client(timeout=60.0)
        self._session = boto3.Session(
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.spapi_aws_region,
        )
        self._cached_access_token: Optional[str] = None
        self._cached_token_expiry_epoch: float = 0.0

    def _now_epoch(self) -> float:
        return datetime.now(timezone.utc).timestamp()

    def _get_lwa_access_token(self) -> str:
        if self._cached_access_token and self._cached_token_expiry_epoch - 60 > self._now_epoch():
            return self._cached_access_token

        response = self._http.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.settings.lwa_refresh_token,
                "client_id": self.settings.lwa_client_id,
                "client_secret": self.settings.lwa_client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"},
        )
        if response.status_code != 200:
            raise SPAPIError(f"Failed to refresh LWA token: {response.status_code} {response.text}")

        payload = response.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        if not token:
            raise SPAPIError("LWA token response did not include access_token")

        self._cached_access_token = token
        self._cached_token_expiry_epoch = self._now_epoch() + expires_in
        return token

    def _signed_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        access_token = self._get_lwa_access_token()
        url = f"{self.settings.spapi_endpoint.rstrip('/')}{path}"

        headers: Dict[str, str] = {
            "host": self.settings.spapi_endpoint.replace("https://", "").replace("http://", ""),
            "x-amz-access-token": access_token,
            "accept": "application/json",
        }
        if json_body is not None:
            headers["content-type"] = "application/json"
        if extra_headers:
            headers.update(extra_headers)

        request = AWSRequest(
            method=method.upper(),
            url=url,
            data=json.dumps(json_body) if json_body is not None else None,
            params=params,
            headers=headers,
        )
        credentials = self._session.get_credentials()
        if credentials is None:
            raise SPAPIError("AWS credentials were not available from boto3 session")

        SigV4Auth(credentials, "execute-api", self.settings.spapi_aws_region).add_auth(request)
        prepared_headers = dict(request.headers.items())

        response = self._http.request(
            method=method.upper(),
            url=url,
            params=params,
            content=json.dumps(json_body) if json_body is not None else None,
            headers=prepared_headers,
        )

        content_type = response.headers.get("content-type", "")
        if response.status_code >= 400:
            raise SPAPIError(
                f"SP-API request failed: {response.status_code} {response.text}"
            )

        if "application/json" in content_type:
            return response.json()
        return {"raw_text": response.text}

    def get_marketplace_participations(self) -> Dict[str, Any]:
        return self._signed_request("GET", "/sellers/v1/marketplaceParticipations")

    def create_report(
        self,
        report_type: str,
        marketplace_ids: List[str],
        data_start_time: Optional[str] = None,
        data_end_time: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "reportType": report_type,
            "marketplaceIds": marketplace_ids,
        }
        if data_start_time:
            body["dataStartTime"] = data_start_time
        if data_end_time:
            body["dataEndTime"] = data_end_time
        return self._signed_request("POST", "/reports/2021-06-30/reports", json_body=body)

    def get_report(self, report_id: str) -> Dict[str, Any]:
        return self._signed_request("GET", f"/reports/2021-06-30/reports/{report_id}")

    def get_report_document(self, report_document_id: str) -> Dict[str, Any]:
        return self._signed_request(
            "GET", f"/reports/2021-06-30/documents/{report_document_id}"
        )

    def download_report_document(self, report_document_id: str) -> Dict[str, Any]:
        doc_meta = self.get_report_document(report_document_id)
        url = doc_meta.get("url")
        compression = doc_meta.get("compressionAlgorithm")
        if not url:
            raise SPAPIError("Report document response did not include a download URL")

        response = self._http.get(url, timeout=120.0)
        if response.status_code >= 400:
            raise SPAPIError(
                f"Failed to download report document: {response.status_code} {response.text}"
            )

        raw_bytes = response.content
        if compression == "GZIP":
            raw_bytes = gzip.decompress(raw_bytes)

        text = raw_bytes.decode("utf-8", errors="replace")
        preview_lines = text.splitlines()[:50]

        return {
            "reportDocumentId": report_document_id,
            "compressionAlgorithm": compression,
            "preview": "\n".join(preview_lines),
            "fullTextBase64": base64.b64encode(raw_bytes).decode("ascii"),
            "note": "fullTextBase64 contains the full downloaded file contents as base64.",
        }

    def get_awd_inventory(
        self,
        next_token: Optional[str] = None,
        sku_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if next_token:
            params["nextToken"] = next_token
        if sku_prefix:
            params["skuPrefix"] = sku_prefix
        return self._signed_request("GET", "/awd/2024-05-09/inventory", params=params)

    def get_fba_inventory_summaries(
        self,
        marketplace_id: Optional[str] = None,
        seller_skus: Optional[List[str]] = None,
        seller_sku: Optional[str] = None,
        details: bool = True,
        start_date_time: Optional[str] = None,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "details": str(details).lower(),
            "granularityType": "Marketplace",
            "granularityId": marketplace_id or self.settings.default_marketplace_id,
        }
        if seller_skus:
            params["sellerSkus"] = ",".join(seller_skus)
        if seller_sku:
            params["sellerSku"] = seller_sku
        if start_date_time:
            params["startDateTime"] = start_date_time
        if next_token:
            params["nextToken"] = next_token
        return self._signed_request("GET", "/fba/inventory/v1/summaries", params=params)


spapi = AmazonSPAPIClient(SETTINGS)


@mcp.tool()
def get_marketplace_participations() -> Dict[str, Any]:
    return spapi.get_marketplace_participations()


@mcp.tool()
def request_inventory_report(
    report_type: str = "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA",
    marketplace_ids: Optional[List[str]] = None,
    data_start_time: Optional[str] = None,
    data_end_time: Optional[str] = None,
) -> Dict[str, Any]:
    marketplace_ids = marketplace_ids or [SETTINGS.default_marketplace_id]
    return spapi.create_report(
        report_type=report_type,
        marketplace_ids=marketplace_ids,
        data_start_time=data_start_time,
        data_end_time=data_end_time,
    )


@mcp.tool()
def get_report_status(report_id: str) -> Dict[str, Any]:
    return spapi.get_report(report_id)


@mcp.tool()
def download_report_document(report_document_id: str) -> Dict[str, Any]:
    return spapi.download_report_document(report_document_id)


@mcp.tool()
def get_awd_inventory(
    next_token: Optional[str] = None,
    sku_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    return spapi.get_awd_inventory(next_token=next_token, sku_prefix=sku_prefix)


@mcp.tool()
def get_fba_inventory_summaries(
    marketplace_id: Optional[str] = None,
    seller_skus: Optional[List[str]] = None,
    seller_sku: Optional[str] = None,
    details: bool = True,
    start_date_time: Optional[str] = None,
    next_token: Optional[str] = None,
) -> Dict[str, Any]:
    return spapi.get_fba_inventory_summaries(
        marketplace_id=marketplace_id,
        seller_skus=seller_skus,
        seller_sku=seller_sku,
        details=details,
        start_date_time=start_date_time,
        next_token=next_token,
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port, path="/mcp")
