
import requests
from bs4 import BeautifulSoup
import json
import urllib.parse
from collections import deque


def scrape_website(base_url):
    # A set to keep track of visited URLs.
    visited = set()
    # A queue for URLs to visit.
    to_visit = deque([base_url])
    # List to store output data.
    data = []

    while to_visit:
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

            # Use BeautifulSoup to parse the page.
            soup = BeautifulSoup(response.text, 'html.parser')
            # Extract text and clean it up.
            text = soup.get_text(separator=' ', strip=True)
            data.append({'url': url, 'text': text})

            # Find and queue the links on this page.
            for link in soup.find_all('a'):
                href = link.get('href')
                if href is None:
                    continue
                # Convert relative URLs to absolute URLs.
                full_url = urllib.parse.urljoin(url, href)
                # Only follow links that start with the base URL.
                if full_url.startswith(base_url) and full_url not in visited:
                    to_visit.append(full_url)

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            continue

    return data


if __name__ == "__main__":
    # Change this to the website you want to scrape.
    base_url = "http://uni-bamberg.de/"
    scraped_data = scrape_website(base_url)

    # Save the scraped data to a JSON file.
    with open("scraped_data.json", "w", encoding="utf-8") as outfile:
        json.dump(scraped_data, outfile, ensure_ascii=False, indent=2)

    print("Scraping completed. Check scraped_data.json for output.")