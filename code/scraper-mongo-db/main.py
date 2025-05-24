import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from web_scraper import WebScraper, logger
from mongodb_handler import  MongoDBHandler


def main():
    """Main execution function"""
    start_time = time.time()

    # Initialize components
    scraper = WebScraper()
    db_handler = MongoDBHandler()

    try:
        # Load or create sitemap
        sitemap_file = "sitemap.json"
        site_map = scraper.load_sitemap_json(sitemap_file)

        if not site_map:
            logger.info("No sitemap found, creating new one...")
            scraper.save_sitemap_json(sitemap_file)
            site_map = scraper.load_sitemap_json(sitemap_file)

        if not site_map:
            logger.error("Could not load sitemap. Exiting.")
            return

        # Process pages with threading
        max_workers = 10  # Adjust based on your needs and server capabilities
        batch_size = 50  # Save in batches for better performance

        scraped_pages = []
        failed_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            future_to_url = {
                executor.submit(scraper.scrape_page, entry): entry
                for entry in site_map
            }

            for future in as_completed(future_to_url):
                try:
                    page_data = future.result()
                    if page_data:
                        scraped_pages.append(page_data)

                        # Save in batches
                        if len(scraped_pages) >= batch_size:
                            db_handler.save_pages_batch(scraped_pages)
                            scraped_pages = []
                    else:
                        failed_count += 1

                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error processing page: {e}")

            # Save remaining pages
            if scraped_pages:
                db_handler.save_pages_batch(scraped_pages)

        # Summary
        total_time = time.time() - start_time
        total_processed = len(site_map)
        successful = total_processed - failed_count

        logger.info(f"""
        Scraping completed!
        Total URLs: {total_processed}
        Successful: {successful}
        Failed: {failed_count}
        Total time: {total_time:.2f} seconds
        Average time per page: {total_time / total_processed:.2f} seconds
        """)

    except Exception as e:
        logger.error(f"Critical error in main execution: {e}")
        raise

    finally:
        db_handler.close()


if __name__ == "__main__":
    main()