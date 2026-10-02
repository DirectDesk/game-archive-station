from html.parser import HTMLParser
from urllib.parse import quote
import re

import httpx


class _DlsiteSearchParser(HTMLParser):
    """Extract the product cards from the small HTML search response."""

    product_pattern = re.compile(r"(?i)(?:RJ|VJ|BJ)\d{6,}")

    def __init__(self):
        super().__init__()
        self.results: list[dict] = []
        self._current: dict | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "a":
            match = self.product_pattern.search(attributes.get("href") or "")
            if match:
                self._current = {"source_id": match.group(0).upper(), "title": "", "cover_url": ""}
                self._text = []
        elif self._current is not None and tag == "img" and not self._current["cover_url"]:
            self._current["cover_url"] = attributes.get("src") or attributes.get("data-src") or ""

    def handle_data(self, data: str) -> None:
        if self._current is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._current is None:
            return
        title = " ".join("".join(self._text).split())
        if title:
            self._current["title"] = title
        self._current["cover_url"] = self._current["cover_url"].strip()
        if self._current["title"]:
            self.results.append(self._current)
        self._current = None
        self._text = []


class DlsiteClient:
    search_url = "https://shturl.cc/bAtR8XXBzwjgcS4NXxfGr5DHExhym4mGIaWcHHJfSV7QyG{}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
    }

    async def search_games(self, query: str) -> list[dict]:
        async with httpx.AsyncClient(timeout=15, headers=self.headers, follow_redirects=True) as client:
            response = await client.get(self.search_url.format(quote(query, safe="")))
            response.raise_for_status()

        parser = _DlsiteSearchParser()
        parser.feed(response.text)
        results = []
        seen = set()
        for item in parser.results:
            source_id = item["source_id"]
            if source_id in seen:
                continue
            seen.add(source_id)
            cover_url = item["cover_url"]
            if cover_url.startswith("//"):
                cover_url = f"https:{cover_url}"
            results.append({
                "source_type": "dlsite",
                "source_id": source_id,
                "title": item["title"],
                "cover_url": cover_url,
            })
        return results