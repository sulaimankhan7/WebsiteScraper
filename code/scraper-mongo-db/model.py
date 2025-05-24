from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime


@dataclass
class ImageData:
    src: str
    title: Optional[str] = None
    alt: Optional[str] = None


@dataclass
class PageData:
    url: str
    content: str
    text: str
    images: List[ImageData]
    scraped_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for MongoDB insertion"""
        return {
            "_id": self.url,
            "url": self.url,
            "content": self.content,
            "text": self.text,
            "images": [img.__dict__ for img in self.images],
            "scraped_at": self.scraped_at
        }


@dataclass
class SitemapEntry:
    link: str
    lastmod: datetime