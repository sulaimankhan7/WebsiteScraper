import json
import requests
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime
import logging
from typing import List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from model import  SitemapEntry, ImageData, PageData
from dataclasses import asdict, dataclass
from urllib.parse import urlparse
from collections import defaultdict
from content_filter import UniversityContentFilter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WebScraper:
    def __init__(self, base_url: str = "https://www.uni-bamberg.de", timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.session = self._create_session()
        self.content_filter = UniversityContentFilter()  # Use the new content filter
        self.content_strainer = SoupStrainer(id="content-main")

    def _create_session(self) -> requests.Session:
        """Create a session with retry strategy and proper headers"""
        session = requests.Session()

        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers to appear as a real browser
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        return session

    def save_sitemap_json(self, filename: str = "sitemap.json") -> None:
        """Save sitemap data to JSON file"""
        logger.info('Starting sitemap extraction...')

        try:
            sitemap_index = self.session.get(f"{self.base_url}/sitemap.xml", timeout=self.timeout)
            sitemap_index.raise_for_status()

            sitemap_strainer = SoupStrainer("sitemap")
            sitemap_link_strainer = SoupStrainer("url")

            sitemap_soup = BeautifulSoup(
                markup=sitemap_index.content,
                parse_only=sitemap_strainer,
                features="xml"
            )

            # Extract individual sitemaps
            sitemap_links = []
            for sitemap in sitemap_soup.contents:
                if hasattr(sitemap, 'loc') and hasattr(sitemap, 'lastmod'):
                    sitemap_links.append({
                        "link": sitemap.loc.text,
                        "lastmod": datetime.fromisoformat(sitemap.lastmod.text),
                    })

            site_links = []

            # Process each sitemap
            for sitemap_link in sitemap_links:
                try:
                    logger.info(f"Processing sitemap: {sitemap_link['link']}")
                    sitemap_content = self.session.get(sitemap_link["link"], timeout=self.timeout)
                    sitemap_content.raise_for_status()

                    sitemap_soup = BeautifulSoup(
                        markup=sitemap_content.content,
                        parse_only=sitemap_link_strainer,
                        features="xml",
                    )

                    # Extract page URLs
                    for url_entry in sitemap_soup.contents:
                        if hasattr(url_entry, 'loc') and hasattr(url_entry, 'lastmod'):
                            site_links.append({
                                "link": url_entry.loc.text,
                                "lastmod": datetime.fromisoformat(url_entry.lastmod.text),
                            })

                    # Add small delay to be respectful
                    time.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error processing sitemap {sitemap_link['link']}: {e}")
                    continue

            # Save to JSON
            grouped_sitemap_entries = self.group_by_first_path_segment(site_links)

            with open(filename, "w", encoding='utf-8') as f:
                json.dump(grouped_sitemap_entries, f, default=str, indent=2, ensure_ascii=False)

            logger.info(f'Sitemap saved with {len(site_links)} URLs.')

        except Exception as e:
            logger.error(f"Error saving sitemap: {e}")
            raise

    def load_sitemap_json(self, filename: str = "sitemap.json") -> dict[str: SitemapEntry]:
        """Load sitemap data from JSON file"""
        logger.info("Loading sitemap from JSON...")

        try:
            with open(filename, "r", encoding='utf-8') as f:
                grouped_data = json.load(f)

            result = {}

            for segment, entries in grouped_data.items():
                result[segment] = [
                    SitemapEntry(
                        link=entry["link"],
                        lastmod=datetime.fromisoformat(entry["lastmod"]) if isinstance(entry["lastmod"], str) else
                        entry["lastmod"]
                    )
                    for entry in entries
                ]

            logger.info(f"Parsed grouped sitemap with {len(result)} segments.")
            return result

        except FileNotFoundError:
            logger.warning(f"Sitemap file {filename} not found.")
            return []
        except Exception as e:
            logger.error(f"Error loading sitemap: {e}")
            return []

    def scrape_page(self, sitemap_entry: SitemapEntry) -> Optional[PageData]:
        """Scrape a single page with improved content filtering"""
        url = sitemap_entry.link
        logger.info(f"Scraping: {url}")

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Parse the full page
            full_soup = BeautifulSoup(response.content, features="lxml")

            # Use the new content filter to extract main content
            main_content = self.content_filter.extract_main_content(full_soup)

            if not main_content:
                logger.warning(f"No main content found for {url}")
                return None

            # Extract images from main content only
            images = self._extract_images_from_content(main_content)

            # Get clean text content
            clean_text = self._get_clean_text(main_content)

            # Create PageData object
            page_data = PageData(
                url=url,
                content=main_content.prettify(),
                text=clean_text,
                images=images,
                scraped_at=datetime.now()
            )

            logger.info(f"Successfully scraped: {url}")
            return page_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error scraping {url}: {e}")
            return None

    def _extract_images_from_content(self, content: BeautifulSoup) -> List[ImageData]:
        """Extract images only from the main content area"""
        images = []

        for img in content.find_all("img"):
            src = img.get("src")
            if src:
                # Convert relative URLs to absolute
                if src.startswith('/'):
                    src = self.base_url + src
                elif src.startswith('./'):
                    src = self.base_url + src[1:]
                elif not src.startswith(('http://', 'https://')):
                    src = self.base_url + '/' + src

                # Skip small images (likely icons or decorative)
                width = img.get('width')
                height = img.get('height')
                if width and height:
                    try:
                        if int(width) < 50 or int(height) < 50:
                            continue
                    except (ValueError, TypeError):
                        pass

                images.append(ImageData(
                    src=src,
                    title=img.get("title"),
                    alt=img.get("alt")
                ))

        return images

    def _get_clean_text(self, content: BeautifulSoup) -> str:
        """Extract clean text with proper formatting"""

        # Add line breaks before certain elements for better text structure
        for element in content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'li']):
            element.insert(0, '\n')

        # Get text and clean it up
        text = content.get_text('\n', strip=True)

        # Clean up multiple newlines and spaces
        import re
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Multiple newlines to double newline
        text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces to single space
        text = re.sub(r'\n[ \t]+', '\n', text)  # Remove spaces after newlines

        return text.strip()

    def group_by_first_path_segment(self, entries):
        """
        Group entries by the first path segment after the domain.

        Args:
            entries: List of SitemapEntry objects or dictionaries with 'link' key

        Returns:
            A dictionary with first path segments as keys and lists of entries as values
        """
        grouped = defaultdict(list)

        try:
            for entry in entries:
                try:
                    # Check if entry is a dictionary or SitemapEntry object
                    if isinstance(entry, dict):
                        if 'link' not in entry:
                            logger.warning(f"Entry missing 'link' field: {entry}")
                            continue
                        link = entry['link']
                    elif hasattr(entry, 'link'):
                        link = entry.link
                    else:
                        logger.warning(f"Invalid entry type: {type(entry)}, expected SitemapEntry or dict with 'link'")
                        continue

                    first_segment = self.get_first_path_segment(link)
                    grouped[first_segment].append(entry)

                except Exception as e:
                    logger.error(f"Error processing entry {entry}: {str(e)}")
                    continue

            return grouped

        except Exception as e:
            logger.error(f"Error in group_by_first_path_segment: {str(e)}")
            return defaultdict(list)

    @staticmethod
    def get_first_path_segment(url):
        # Parse the URL
        parsed_url = urlparse(url)

        # Get the path
        path = parsed_url.path

        # Split the path and get the first non-empty segment
        segments = path.strip('/').split('/')
        if segments and segments[0]:
            return segments[0]
        return "/"

