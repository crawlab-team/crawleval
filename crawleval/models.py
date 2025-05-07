from datetime import UTC, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class PageData(BaseModel):
    html: str
    title: str
    url: str
    screenshot: Optional[bytes] = None


class PageMetadata(BaseModel):
    id: str
    name: str
    description: str
    url: str
    created_at: datetime = Field(default_factory=lambda _: datetime.now(UTC))
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
