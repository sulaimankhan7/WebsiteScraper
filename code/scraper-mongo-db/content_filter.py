"""
Content filtering configuration for different website types
"""
from typing import Dict, List, Set
from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag
import re


@dataclass
class ContentFilterConfig:
    """Configuration for content filtering"""
    # CSS selectors for main content (in priority order)
    main_content_selectors: List[str]

    # Elements to remove completely
    remove_tags: List[str]

    # Class/ID patterns to remove
    remove_patterns: List[str]

    # Minimum text length for valid content blocks
    min_text_length: int = 50

    # Skip images smaller than these dimensions
    min_image_width: int = 50
    min_image_height: int = 50


class UniversityContentFilter:
    """Specialized content filter for university websites"""

    def __init__(self):
        self.config = ContentFilterConfig(
            main_content_selectors=[
                # University of Bamberg specific
                'main[role="main"] .content-wrapper',
                '#content-main .main-content',
                '#content-main article',
                '.page-content .content-area',

                # Generic academic site patterns
                'main[role="main"]',
                'article.main-content',
                '.main-content-area',
                '.page-content',
                '.content-wrapper',

                # Fallback selectors
                'main',
                'article',
                '[role="main"]',
                '#content',
                '.content'
            ],

            remove_tags=[
                'nav', 'aside', 'header', 'footer', 'script', 'style', 'noscript'
            ],

            remove_patterns=[
                # Navigation and menu elements
                'nav', 'navigation', 'menu', 'breadcrumb', 'breadcrumbs',
                'main-nav', 'primary-nav', 'secondary-nav',

                # Sidebar and widget elements
                'sidebar', 'side-bar', 'widget', 'aside-content',

                # Social and sharing
                'social', 'share', 'sharing', 'share-buttons',

                # Metadata and administrative
                'meta-info', 'post-meta', 'entry-meta', 'page-meta',
                'last-modified', 'publish-date', 'author-info',

                # University specific
                'contact-info', 'address-block', 'office-hours',
                'quick-links', 'related-links', 'see-also',

                # Comments and interaction
                'comment', 'comments', 'feedback', 'rating',

                # Pagination and navigation
                'pagination', 'pager', 'prev-next', 'page-nav',

                # Tags and categories
                'tags', 'tag-list', 'categories', 'category-list',

                # Promotional content
                'advertisement', 'ad-', 'promo', 'banner', 'sponsored',

                # Search and filters
                'search-form', 'filter', 'sort-options',

                # University portal specific
                'portal-', 'login-', 'user-menu', 'account-',
                'language-switcher', 'lang-'
            ]
        )

    def extract_main_content(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Extract main content using university-specific filtering"""

        # Try to find main content using selectors
        main_content = None

        for selector in self.config.main_content_selectors:
            elements = soup.select(selector)
            if elements:
                # Choose the element with the most text content
                best_element = max(elements, key=lambda x: len(x.get_text(strip=True)))
                if len(best_element.get_text(strip=True)) > self.config.min_text_length:
                    main_content = best_element
                    break

        # Fallback: use the largest content block
        if not main_content:
            main_content = self._find_largest_content_block(soup)

        if not main_content:
            return None

        # Create a copy to avoid modifying the original
        content_copy = BeautifulSoup(str(main_content), 'lxml')

        # Apply filtering
        self._remove_unwanted_elements(content_copy)
        self._clean_university_specific_content(content_copy)

        return content_copy

    def _find_largest_content_block(self, soup: BeautifulSoup) -> Tag:
        """Find the element with the most substantial text content"""
        candidates = []

        # Look for common content containers
        for selector in ['div', 'section', 'article', 'main']:
            for element in soup.find_all(selector):
                text_length = len(element.get_text(strip=True))
                if text_length > self.config.min_text_length:
                    candidates.append((element, text_length))

        if candidates:
            # Return the element with the most text
            return max(candidates, key=lambda x: x[1])[0]

        return None

    def _remove_unwanted_elements(self, content: BeautifulSoup) -> None:
        """Remove unwanted elements based on configuration"""

        # Remove by tag name
        for tag in self.config.remove_tags:
            for element in content.find_all(tag):
                element.decompose()

        # Remove by class and id patterns
        for pattern in self.config.remove_patterns:
            # Remove by class (case-insensitive partial match)
            for element in content.find_all(attrs={'class': lambda x: self._matches_pattern(x, pattern)}):
                element.decompose()

            # Remove by id (case-insensitive partial match)
            for element in content.find_all(attrs={'id': lambda x: self._matches_pattern(x, pattern)}):
                element.decompose()

        # Remove elements with minimal content
        self._remove_low_content_elements(content)

    def _matches_pattern(self, attr_value, pattern: str) -> bool:
        """Check if attribute value matches the removal pattern"""
        if not attr_value:
            return False

        if isinstance(attr_value, list):
            attr_string = ' '.join(attr_value).lower()
        else:
            attr_string = str(attr_value).lower()
        return pattern.lower() in attr_string

    def _remove_low_content_elements(self, content: BeautifulSoup) -> None:
        """Remove elements with very little meaningful content"""

        for element in content.find_all():
            if element.name in ['img', 'br', 'hr']:  # Keep these regardless
                continue

            text = element.get_text(strip=True)

            # Remove if too short and no images
            if len(text) < 10 and not element.find('img'):
                element.decompose()
                continue

            # Remove if only whitespace or common filler text
            if not text or text.lower() in ['mehr', 'more', 'weiterlesen', 'read more', '...', 'hier']:
                element.decompose()

    def _clean_university_specific_content(self, content: BeautifulSoup) -> None:
        """Remove university-specific unwanted content patterns"""

        # Remove elements containing only dates without context
        date_pattern = re.compile(r'^\s*\d{1,2}\.\s*\w+\s*\d{4}\s*$')

        for element in content.find_all(text=date_pattern):
            parent = element.parent
            if parent and len(parent.get_text(strip=True)) == len(element.strip()):
                parent.decompose()

        # Remove standalone contact information blocks
        contact_patterns = [
            r'tel\.\s*\+\d+',
            r'email:\s*\S+@\S+',
            r'raum\s*\d+',
            r'sprechstunden?:'
        ]

        for pattern in contact_patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            for element in content.find_all(text=regex):
                parent = element.parent
                if parent and len(parent.get_text(strip=True)) < 100:
                    parent.decompose()
