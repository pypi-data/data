from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import Self

import httpx
import pydantic
import structlog
from github import Github
from pydantic import HttpUrl
from tenacity import AsyncRetrying, stop_after_attempt, wait_random_exponential

log = structlog.get_logger()


class RepositoryPackage(pydantic.BaseModel):
    project_name: str
    project_version: str
    url: HttpUrl
    upload_time: datetime


class RepositoryIndex(pydantic.BaseModel):
    packages: list[RepositoryPackage]

    def __str__(self):
        return f"RepositoryIndex({len(self.packages)} packages)"

    def __repr__(self):
        return str(self)


class CodeRepository(pydantic.BaseModel):
    name: str
    url: HttpUrl
    ssh_url: str
    parquet_url: HttpUrl
    index: RepositoryIndex | None = None

    @pydantic.computed_field()
    @property
    def index_url(self) -> HttpUrl:
        return HttpUrl(f"https://raw.githubusercontent.com/pypi-data/{self.name}/refs/heads/main/index.json")

    @pydantic.computed_field()
    @property
    def dataset_url(self) -> HttpUrl:
        return HttpUrl(f"{self.url}/releases/download/latest/dataset.parquet")

    @pydantic.computed_field()
    @property
    def number(self) -> int:
        return int(self.name.split("-")[-1])

    @classmethod
    def fetch_all(cls, github: Github) -> list[Self]:
        return [
            cls(
                name=repo.name,
                url=repo.html_url,
                ssh_url=repo.ssh_url,
                parquet_url=f"{repo.html_url}/releases/download/latest/dataset.parquet",
            )
            for repo in github.get_organization("pypi-data").get_repos()
            if repo.name.startswith("pypi-mirror-")
        ]

    async def _make_request(self, client: httpx.AsyncClient, url: HttpUrl,
                            follow_redirects: bool = True) -> httpx.Response | None:
        async for attempt in AsyncRetrying(stop=stop_after_attempt(3),
                                           wait=wait_random_exponential(multiplier=1, max=4)):
            with attempt:
                response = await client.get(str(url), headers={'Accept-Encoding': 'gzip'},
                                            follow_redirects=follow_redirects)
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == HTTPStatus.NOT_FOUND:
                        log.warning(f"URL {url} not found for {self.name}")
                        return None
                    if not follow_redirects and e.response.is_redirect:
                        return e.response
                    raise
        return response

    async def get_temporary_dataset_url(self, client: httpx.AsyncClient) -> str | None:
        response = await self._make_request(client, self.dataset_url, follow_redirects=False)
        if response is None:
            return None
        return response.headers["Location"]

    async def with_index(self, client: httpx.AsyncClient) -> Self:
        response = await self._make_request(client, self.index_url)
        if response is None:
            return self.model_copy()
        return self.model_copy(update={'index': RepositoryIndex.model_validate_json(response.content)})

    async def download_dataset(self, client: httpx.AsyncClient, output: Path):
        async with client.stream("GET", str(self.dataset_url)) as resp:
            with output.open("wb") as f:
                async for buffer in resp.aiter_bytes():
                    f.write(buffer)

    def without_index(self) -> Self:
        return self.model_copy(update={'index': None})