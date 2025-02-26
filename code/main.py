import requests
from bs4 import BeautifulSoup
import json
import urllib.parse
from collections import deque
from datetime import datetime
import os


def ensure_directories():
    os.makedirs("input", exist_ok=True)
    os.makedirs("output", exist_ok=True)


def load_visited(file_path):
    """Load visited URLs from a file into a set."""
    visited = set()
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if url:
                    visited.add(url)
    return visited


def save_visited(file_path, visited):
    """Save visited URLs to a file (one URL per line)."""
    with open(file_path, 'w', encoding="utf-8") as f:
        for url in sorted(visited):
            f.write(url + "\n")


def scrape_website(base_url, max_pages=100, visited_file="input/visited_urls.txt"):
    # Load previously visited URLs.
    visited = load_visited(visited_file)
    # A deque for URLs to visit, initializing with base_url if not already visited.
    to_visit = deque([])
    if base_url not in visited:
        to_visit.append(base_url)
    # List to store output data (scraped results).
    data = []
    # Counter for the number of scraped pages.
    count = 0

    while to_visit and count < max_pages:
        url = to_visit.popleft()
        if url in visited:
            continue
        print(f"Scraping: {url}")
        visited.add(url)
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                print(f"Skipping {url} due to response status: {response.status_code}")
                continue

            # Parse the page.
            soup = BeautifulSoup(response.text, 'html.parser')
            # Extract text.
            text = soup.get_text(separator=' ', strip=True)
            data.append({'url': url, 'text': text})
            count += 1

            # If we've reached the max_pages limit, break out.
            if count >= max_pages:
                break

            # Queue the internal links.
            for link in soup.find_all('a'):
                href = link.get('href')
                if href is None:
                    continue
                full_url = urllib.parse.urljoin(url, href)
                if full_url.startswith(base_url) and full_url not in visited:
                    to_visit.append(full_url)

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            continue

    # Save the updated visited URLs.
    save_visited(visited_file, visited)
    return data


if __name__ == "__main__":
    # Ensure that input and output directories exist.
    ensure_directories()

    # Website to scrape.
    base_url = "http://uni-bamberg.de/"
    # Set the maximum number of pages to scrape.
    max_pages = 100
    # File to store visited URLs, stored in the input folder.
    visited_file = "input/visited_urls.txt"

    scraped_data = scrape_website(base_url, max_pages, visited_file)

    # Create a timestamp for the output file name.
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"output/scraped_data_{timestamp}.json"

    # Save the scraped data to a timestamped JSON file in the output folder.
    with open(output_filename, "w", encoding="utf-8") as outfile:
        json.dump(scraped_data, outfile, ensure_ascii=False, indent=2)

    print(f"Scraping completed. Check {output_filename} for output.")