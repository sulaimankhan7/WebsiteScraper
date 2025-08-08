import pymongo
from pymongo.errors import PyMongoError
from typing import List, Optional, Dict
from model import PageData, ImageData, SitemapEntry
import logging

logger = logging.getLogger(__name__)


class MongoDBHandler:
    def __init__(self, connection_string: str = "mongodb://localhost:27017/",
                 database_name: str = "rag2", collection_name: str = "pages"):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.database = None
        self.collection = None
        self.connect()

    def connect(self):
        """Establish MongoDB connection"""
        try:
            self.client = pymongo.MongoClient(self.connection_string)
            self.database = self.client[self.database_name]
            self.collection = self.database[self.collection_name]

            # Test connection
            self.client.admin.command('ping')
            logger.info("Connected to MongoDB successfully")

        except PyMongoError as e:
            logger.error(f"MongoDB connection error: {e}")
            raise

    def save_page(self, page_data: PageData, category: str = None) -> bool:
        """Save a single page to MongoDB with category"""
        try:
            page_dict = page_data.to_dict()
            if category:
                page_dict['category'] = category

            result = self.collection.update_one(
                {"_id": page_data.url},
                {"$set": page_dict},
                upsert=True
            )

            if result.upserted_id or result.modified_count > 0:
                logger.info(f"Saved page: {page_data.url}")
                return True
            else:
                logger.warning(f"No changes made for: {page_data.url}")
                return False

        except PyMongoError as e:
            logger.error(f"Error saving page {page_data.url}: {e}")
            return False

    def save_pages_batch(self, pages: List[PageData], category: str = None) -> int:
        """Save multiple pages in batch with category"""
        if not pages:
            return 0

        try:
            operations = []
            for page in pages:
                page_dict = page.to_dict()
                if category:
                    page_dict['category'] = category

                operations.append(
                    pymongo.UpdateOne(
                        {"_id": page.url},
                        {"$set": page_dict},
                        upsert=True
                    )
                )

            result = self.collection.bulk_write(operations)
            saved_count = result.upserted_count + result.modified_count
            logger.info(f"Batch saved {saved_count} pages" + (f" in category '{category}'" if category else ""))
            return saved_count

        except PyMongoError as e:
            logger.error(f"Error in batch save: {e}")
            return 0

    def save_pages_by_category(self, pages_by_category: Dict[str, List[PageData]]) -> Dict[str, int]:
        """Save pages grouped by category"""
        if not pages_by_category:
            return {}

        results = {}

        for category, pages in pages_by_category.items():
            if not pages:
                continue

            saved = self.save_pages_batch(pages, category)
            results[category] = saved
            logger.info(f"Saved {saved} pages in category '{category}'")

        return results

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")