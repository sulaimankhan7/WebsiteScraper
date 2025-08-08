import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from web_scraper import WebScraper, logger
from mongodb_handler import MongoDBHandler
from collections import defaultdict
from content_validator import ContentQualityValidator, print_validation_report


def main():
    """Main execution function"""
    start_time = time.time()

    # Initialize components
    scraper = WebScraper()
    db_handler = MongoDBHandler()
    validator = ContentQualityValidator()  # Add validator

    try:
        # Load or create sitemap
        sitemap_file = "sitemap.json"
        sitemap_entries = scraper.load_sitemap_json(sitemap_file)

        if not sitemap_entries:
            logger.info("No sitemap found, creating new one...")
            scraper.save_sitemap_json(sitemap_file)
            sitemap_entries = scraper.load_sitemap_json(sitemap_file)

        if not sitemap_entries:
            logger.error("Could not load sitemap. Exiting.")
            return

        # Process pages with threading
        max_workers = 16  # Adjust based on your needs and server capabilities
        batch_size = 50  # Save in batches for better performance

        scraped_pages_by_category = defaultdict(list)
        all_scraped_pages = []  # For validation
        failed_count = 0
        total_entries = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks - iterate through the dictionary
            future_to_data = {}

            for category, entries in sitemap_entries.items():
                logger.info(f"Scheduling scraping for category: {category} with {len(entries)} pages")
                total_entries += len(entries)

                for entry in entries:
                    future = executor.submit(scraper.scrape_page, entry)
                    future_to_data[future] = (entry, category)

            for future in as_completed(future_to_data):
                try:
                    page_data = future.result()
                    entry, category = future_to_data[future]

                    if page_data:
                        scraped_pages_by_category[category].append(page_data)
                        all_scraped_pages.append(page_data)  # Collect for validation

                        # Save in batches when a category reaches the batch size
                        if len(scraped_pages_by_category[category]) >= batch_size:
                            db_handler.save_pages_batch(scraped_pages_by_category[category], category)
                            logger.info(
                                f"Saved batch of {len(scraped_pages_by_category[category])} pages in category '{category}'")
                            scraped_pages_by_category[category] = []
                    else:
                        failed_count += 1

                except Exception as e:
                    failed_count += 1
                    entry, category = future_to_data[future]
                    logger.error(f"Error processing page': {e}")

            # Save remaining pages for each category
            for category, pages in scraped_pages_by_category.items():
                if pages:
                    db_handler.save_pages_batch(pages, category)
                    logger.info(f"Saved final batch of {len(pages)} pages in category '{category}'")

        # Validate content quality
        if all_scraped_pages:
            logger.info("Starting content quality validation...")
            validation_results = validator.validate_batch(all_scraped_pages)
            print_validation_report(validation_results)

        # Summary
        total_time = time.time() - start_time
        successful = total_entries - failed_count

        logger.info(f"""
        Scraping completed!
        Total URLs: {total_entries}
        Successful: {successful}
        Failed: {failed_count}
        Categories: {', '.join(sitemap_entries.keys())}
        Total time: {total_time:.2f} seconds
        Average time per page: {total_time / total_entries:.2f} seconds
        """)

    except Exception as e:
        logger.error(f"Critical error in main execution: {e}")
        raise

    finally:
        db_handler.close()


if __name__ == "__main__":
    main()