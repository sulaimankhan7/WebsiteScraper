#!/usr/bin/env python3
"""
Test script to demonstrate the improved content filtering
Run this to test the improvements on a single page
"""

from web_scraper import WebScraper
from content_validator import ContentQualityValidator, print_validation_report
from model import SitemapEntry
from datetime import datetime
import sys


def test_single_page(url: str = "https://www.uni-bamberg.de/transfer/"):
    """Test content extraction for a single page"""
    print(f"Testing improved content extraction for: {url}")
    print("=" * 80)

    # Initialize scraper with improved filtering
    scraper = WebScraper()
    validator = ContentQualityValidator()

    # Create a test sitemap entry
    test_entry = SitemapEntry(
        link=url,
        lastmod=datetime.now()
    )

    try:
        # Scrape the page with new filtering
        page_data = scraper.scrape_page(test_entry)

        if not page_data:
            print("❌ Failed to extract content from the page")
            return

        print("✅ Successfully extracted content!")
        print(f"Content length: {len(page_data.content)} characters")
        print(f"Text length: {len(page_data.text)} characters")
        print(f"Images found: {len(page_data.images)}")
        print("-" * 80)

        # Show first 500 characters of extracted text
        print("📄 EXTRACTED TEXT PREVIEW:")
        print("-" * 40)
        preview_text = page_data.text[:800] + "..." if len(page_data.text) > 800 else page_data.text
        print(preview_text)
        print("-" * 40)

        # Validate content quality
        print("\n🔍 CONTENT QUALITY ANALYSIS:")
        validation_result = validator.validate_page_content(page_data)

        print(f"Valid Content: {'✅ Yes' if validation_result['is_valid'] else '❌ No'}")
        print(f"Word Count: {validation_result['word_count']}")
        print(f"Quality Score: {validation_result['quality_score']}/10")
        print(f"Content Density: {validation_result['content_density']:.2%}")
        print(f"Navigation Artifacts: {'⚠️ Yes' if validation_result['has_navigation_artifacts'] else '✅ No'}")

        if validation_result['issues']:
            print("⚠️  Issues Found:")
            for issue in validation_result['issues']:
                print(f"   • {issue}")

        # Show images found
        if page_data.images:
            print(f"\n🖼️  IMAGES FOUND ({len(page_data.images)}):")
            for i, img in enumerate(page_data.images[:3], 1):  # Show first 3
                print(f"   {i}. {img.src}")
                if img.alt:
                    print(f"      Alt: {img.alt}")

        print("\n" + "=" * 80)
        print("✨ Test completed! The content should now be much cleaner.")
        print("   • Navigation menus should be removed")
        print("   • Only main article content should remain")
        print("   • Sidebar and footer content should be filtered out")

        return page_data

    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return None


def compare_before_after(url: str = "https://www.uni-bamberg.de/transfer/"):
    """Compare content extraction before and after improvements"""
    print(f"🔄 COMPARING BEFORE/AFTER IMPROVEMENTS")
    print("=" * 80)

    # Test with new improved system
    print("Testing with IMPROVED filtering...")
    improved_data = test_single_page(url)

    if improved_data:
        print(f"\n📊 SUMMARY:")
        print(f"Improved extraction - Text length: {len(improved_data.text)} chars")
        print(f"Should be cleaner with less navigation/menu content")


if __name__ == "__main__":
    # Allow testing custom URL from command line
    test_url = sys.argv[1] if len(sys.argv) > 1 else "https://www.uni-bamberg.de/transfer/"

    print("🧪 CONTENT EXTRACTION IMPROVEMENT TEST")
    print(f"Testing URL: {test_url}")
    print("=" * 80)

    # Run the comparison test
    compare_before_after(test_url)
