import asyncio
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

import httpx

from .translator_base import TranslatorBase


class TencentTranslator(TranslatorBase):
    host = "tmt.tencentcloudapi.com"
    service = "tmt"
    version = "2018-03-21"
    action = "TextTranslate"

    def __init__(self, secret_id: str = "", secret_key: str = "", region: str = ""):
        self.secret_id = secret_id or os.getenv("TENCENT_SECRET_ID", "")
        self.secret_key = secret_key or os.getenv("TENCENT_SECRET_KEY", "")
        self.region = region or os.getenv("TENCENT_REGION", "ap-guangzhou")
        self._lock = asyncio.Lock()
        self._last_request = 0.0

    @staticmethod
    def _sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode(), hashlib.sha256).digest()

    async def translate(self, text: str, source: str = "en", target: str = "zh") -> str:
        if not text or not self.secret_id or not self.secret_key:
            return text
        try:
            payload = json.dumps({"SourceText": text[:5000], "Source": source, "Target": target, "ProjectId": 0}, separators=(",", ":"))
            timestamp = int(datetime.now(timezone.utc).timestamp())
            date = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")
            canonical_headers = f"content-type:application/json; charset=utf-8\nhost:{self.host}\n"
            signed_headers = "content-type;host"
            canonical_request = f"POST\n/\n\n{canonical_headers}\n{signed_headers}\n{hashlib.sha256(payload.encode()).hexdigest()}"
            credential_scope = f"{date}/{self.service}/tc3_request"
            string_to_sign = f"TC3-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
            secret_date = self._sign(f"TC3{self.secret_key}".encode(), date)
            secret_service = self._sign(secret_date, self.service)
            secret_signing = self._sign(secret_service, "tc3_request")
            signature = hmac.new(secret_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
            authorization = f"TC3-HMAC-SHA256 Credential={self.secret_id}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
            async with self._lock:
                delay = 0.2 - (asyncio.get_running_loop().time() - self._last_request)
                if delay > 0:
                    await asyncio.sleep(delay)
                self._last_request = asyncio.get_running_loop().time()
                async with httpx.AsyncClient(timeout=15) as client:
                    response = await client.post(f"https://{self.host}", content=payload, headers={"Authorization": authorization, "Content-Type": "application/json; charset=utf-8", "Host": self.host, "X-TC-Action": self.action, "X-TC-Version": self.version, "X-TC-Region": self.region, "X-TC-Timestamp": str(timestamp)})
            return response.json().get("Response", {}).get("TargetText", text) if response.is_success else text
        except Exception:
            return text

    async def batch_translate(self, texts: list[str], source: str = "en", target: str = "zh") -> list[str]:
        return [await self.translate(text, source, target) for text in texts]