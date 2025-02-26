import requests
from bs4 import BeautifulSoup
import json
import urllib.parse
from collections import deque
from datetime import datetime
import os

# Define 1GB in bytes
ONE_GB = 1_073_741_824


def ensure_directories():
    os.makedirs("input", exist_ok=True)
    os.makedirs("output", exist_ok=True)


def load_visited(file_path):
    """Load visited URLs from a file into a set."""
    visited = set()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if url:
                    visited.add(url)
    return visited


def save_visited(file_path, visited):
    """Save visited URLs to a file (one URL per line)."""
    with open(file_path, "w", encoding="utf-8") as f:
        for url in sorted(visited):
            f.write(url + "\n")


def load_pending(file_path):
    """Load pending URLs from a file into a deque."""
    pending = deque()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if url:
                    pending.append(url)
    return pending


def save_pending(file_path, pending):
    """Save pending URLs to a file (one URL per line)."""
    with open(file_path, "w", encoding="utf-8") as f:
        for url in pending:
            f.write(url + "\n")


def flush_data(data, batch_index):
    """Flush the data into a JSON file and return the filename."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"output/scraped_data_{timestamp}_batch{batch_index}.json"
    with open(output_filename, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, ensure_ascii=False, indent=2)
    print(f"Flushed {len(data)} records to {output_filename}")
    return output_filename


def scrape_website(base_url, max_pages=0, visited_file="input/visited_urls.txt", pending_file="input/pending_urls.txt"):
    """
    Scrape the website starting at base_url.

    If max_pages is set to 0, the scraper runs until no more pending links remain.
    Otherwise, it stops after scraping max_pages pages.
    """
    # Load previously visited URLs and pending URLs.
    visited = load_visited(visited_file)
    to_visit = load_pending(pending_file)

    # If base_url is not visited and not in the pending list, add it.
    if base_url not in visited and base_url not in to_visit:
        to_visit.append(base_url)

    data = []  # List to store the scraped data.
    batch_index = 1  # Batch counter for JSON flushing.
    count = 0  # Counter for the number of scraped pages.

    # Continue scraping while there are URLs to visit and max_pages condition holds.
    # If max_pages is 0, it will ignore the count check.
    while to_visit and (max_pages == 0 or count < max_pages):
        url = to_visit.popleft()
        if url in visited:
            continue

        print(f"Scraping: {count} {url}")
        visited.add(url)
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                print(f"Skipping {url} due to response status: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            text = soup.get_text(separator=" ", strip=True)
            data.append({"url": url, "text": text})
            count += 1

            # Queue the internal links.
            for link in soup.find_all("a"):
                href = link.get("href")
                if not href:
                    continue
                full_url = urllib.parse.urljoin(url, href)
                if full_url.startswith(base_url) and full_url not in visited and full_url not in to_visit:
                    to_visit.append(full_url)

            # Check if the accumulated data has reached 1GB in size.
            current_size = len(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            if current_size >= ONE_GB:
                flush_data(data, batch_index)
                data = []  # Clear data after flushing.
                batch_index += 1

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            continue

    # Flush any remaining data even if it hasn't reached 1GB.
    if data:
        flush_data(data, batch_index)

    # Save the updated visited URLs and pending URLs.
    save_visited(visited_file, visited)
    save_pending(pending_file, to_visit)
    return count


if __name__ == "__main__":
    ensure_directories()

    base_url = "http://uni-bamberg.de/"
    # Set max_pages to 0 to scrape the whole website, or any positive integer to limit the pages.
    max_pages = 0
    visited_file = "input/visited_urls.txt"
    pending_file = "input/pending_urls.txt"

    total_scraped = scrape_website(base_url, max_pages, visited_file, pending_file)
    print(f"Scraping completed. Total pages scraped: {total_scraped}.")