from datetime import UTC, datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class PageData(BaseModel):
    html: str
    title: str
    url: str
    screenshot: Optional[bytes] = None
    file_id: Optional[str] = None


class PageMetadata(BaseModel):
    id: str
    name: str
    description: str
    url: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    size: int = 0
    dom_nodes: int = 0
    text_nodes: int = 0
    link_nodes: int = 0
    image_nodes: int = 0
    max_nesting_level: int = 0


class FieldRule(BaseModel):
    name: str
    selector: str


class ListRule(BaseModel):
    name: str
    selector: str


class Pagination(BaseModel):
    selector: str


class PagePattern(BaseModel):
    single_fields: List[FieldRule] = []
    lists: List[ListRule] = []
    pagination: Optional[Pagination] = None


class PageGroundTruth(BaseModel):
    question: str


class UrlProcessResult(BaseModel):
    """Result of processing a single URL"""

    url: str
    status: Literal[
        "success", "duplicate_url", "duplicate_content", "failed", "error", "unknown"
    ]
    file_id: Optional[str] = None
    is_new: bool = False
    error: Optional[str] = None


class BatchProcessingResults(BaseModel):
    """Summary of batch processing results"""

    total: int = 0
    successful: int = 0
    failed: int = 0
    duplicate_urls: int = 0
    duplicate_content: int = 0
    new_entries: int = 0
    entries: List[UrlProcessResult] = []
