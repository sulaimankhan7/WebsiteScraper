import requests
from bs4 import BeautifulSoup, SoupStrainer

import json
from datetime import datetime

from multiprocessing import Pool
import pymongo
from Data import PageData

content_strainer = SoupStrainer(id="content-main")


def saveSiteMapJSON():
    sitemap_index = requests.get("https://www.uni-bamberg.de/sitemap.xml")
    sitemap_strainer = SoupStrainer("sitemap")
    sitemap_link_strainer = SoupStrainer("url")
    sitemap_soup = BeautifulSoup(
        markup=sitemap_index.content, parse_only=sitemap_strainer, features="xml"
    )
    # Die einzelnen Sitemaps und deren letztes Änderungsdatum aus der globalen Sitemap auslesen und als Liste ausgeben
    sitemap_links = list(
        map(
            lambda n: {
                "link": n.loc.text,
                "lastmod": datetime.fromisoformat(n.lastmod.text),
            },
            sitemap_soup.contents,
        )
    )
    site_links = list()
    # Durch die einzelnen Sitemaps durchgehen
    for sitemap_link in sitemap_links:
        sitemap_content = BeautifulSoup(
            markup=requests.get(sitemap_link.get("link")).content,
            parse_only=sitemap_link_strainer,
            features="xml",
        )
        # Die Seiten aus der aktuellen Sitemap auslesen und mit dem Zeitpunkt der letzten Änderung ablegen
        site_links.extend(
            list(
                map(
                    lambda n: {
                        "link": n.loc.text,
                        "lastmod": datetime.fromisoformat(n.lastmod.text),
                    },
                    sitemap_content.contents,
                )
            )
        )
    # Die ausgelesenen Links als JSON abspeicher, damit man nicht jedes Mal die Sitemaps erneut abrufen muss (kann manchmal dauern, da die z.T. wohl erst bei Zugriff ereugt werden)
    f = open("sitemap.json", "w")
    json.dump(site_links, fp=f, default=str)
    f.close()


def loadSiteMapJSON() -> list:
    with open(file="sitemap.json", mode="r") as f:
        sitemaps = json.load(f)
    return sitemaps
import requests
from bs4 import BeautifulSoup, SoupStrainer

import json
from datetime import datetime

from multiprocessing import Pool
import pymongo
from dotenv import dotenv_values
from Data import PageData

content_strainer = SoupStrainer(id="content-main")


def saveSiteMapJSON():
    print('Save site map started...')
    sitemap_index = requests.get("https://www.uni-bamberg.de/sitemap.xml")
    sitemap_strainer = SoupStrainer("sitemap")
    sitemap_link_strainer = SoupStrainer("url")
    sitemap_soup = BeautifulSoup(
        markup=sitemap_index.content, parse_only=sitemap_strainer, features="xml"
    )
    # Die einzelnen Sitemaps und deren letztes Änderungsdatum aus der globalen Sitemap auslesen und als Liste ausgeben
    sitemap_links = list(
        map(
            lambda n: {
                "link": n.loc.text,
                "lastmod": datetime.fromisoformat(n.lastmod.text),
            },
            sitemap_soup.contents,
        )
    )
    site_links = list()
    # Durch die einzelnen Sitemaps durchgehen
    for sitemap_link in sitemap_links:
        sitemap_content = BeautifulSoup(
            markup=requests.get(sitemap_link.get("link")).content,
            parse_only=sitemap_link_strainer,
            features="xml",
        )
        # Die Seiten aus der aktuellen Sitemap auslesen und mit dem Zeitpunkt der letzten Änderung ablegen
        site_links.extend(
            list(
                map(
                    lambda n: {
                        "link": n.loc.text,
                        "lastmod": datetime.fromisoformat(n.lastmod.text),
                    },
                    sitemap_content.contents,
                )
            )
        )
    # Die ausgelesenen Links als JSON abspeicher, damit man nicht jedes Mal die Sitemaps erneut abrufen muss (kann manchmal dauern, da die z.T. wohl erst bei Zugriff ereugt werden)
    f = open("sitemap.json", "w")
    json.dump(site_links, fp=f, default=str)
    f.close()
    print('Site map saved.')


def loadSiteMapJSON() -> list:
    print("Load site map started...")
    with open(file="sitemap.json", mode="r") as f:
        sitemaps = json.load(f)
    print("Site map loaded.")
    return sitemaps


def loadSite(url: str) -> PageData:
    print("Load site "+ url)
    temp = requests.get(url.get("link")).content
    soup = BeautifulSoup(
        markup=temp,
        parse_only=content_strainer,
        features="lxml",
    )
    try:
        images = list(
            map(
                lambda n: {
                    "src": n.get("src"),
                    "title": n.get("title"),
                    "alt": n.get("alt"),
                },
                soup.findAll("img"),
            )
        )
    except:
        images = list()
    data = PageData(
        url["link"], str(soup.prettify()), soup.get_text(" ", strip=True), images
    )
    print("Site data loaded.")
    return data


if __name__ == "__main__":
    saveSiteMapJSON()
    sm = loadSiteMapJSON()

    import time

    st = time.time()
    pool = Pool()
    vars = dotenv_values(".env")

    myclient = myclient = pymongo.MongoClient()

    mydb = myclient["rag"]
    mycol = mydb["pages"]

    # st = time.time()
    timestamp = datetime.now().replace(microsecond=0).isoformat()
    for page in pool.imap(loadSite, sm, 100):
        # print(page.url)
        mycol.update_one(
            filter={"_id": page.url},
            update={
                "$set": {
                    "_id": page.url,
                    "last_update": timestamp,
                    "content": page.content,
                    "text": page.text,
                    "images": page.images,
                }
            },
            upsert=True,
        )
    end = time.time()
    print((end - st))
    pool.close()


def loadSite(url: str) -> PageData:
    temp = requests.get(url.get("link")).content
    soup = BeautifulSoup(
        markup=temp,
        parse_only=content_strainer,
        features="lxml",
    )
    try:
        images = list(
            map(
                lambda n: {
                    "src": n.get("src"),
                    "title": n.get("title"),
                    "alt": n.get("alt"),
                },
                soup.findAll("img"),
            )
        )
    except:
        images = list()
    data = PageData(
        url["link"], str(soup.prettify()), soup.get_text(" ", strip=True), images
    )
    return data


if __name__ == "__main__":
    saveSiteMapJSON()
    sm = loadSiteMapJSON()

    import time

    st = time.time()
    pool = Pool()


    myclient = pymongo.MongoClient()

    mydb = myclient["rag"]
    mycol = mydb["pages"]

    # st = time.time()
    timestamp = datetime.now().replace(microsecond=0).isoformat()
    for page in pool.imap(loadSite, sm, 100):
        # print(page.url)
        mycol.update_one(
            filter={"_id": page.url},
            update={
                "$set": {
                    "_id": page.url,
                    "last_update": timestamp,
                    "content": page.content,
                    "text": page.text,
                    "images": page.images,
                }
            },
            upsert=True,
        )
    end = time.time()
    print((end - st))
    pool.close()
