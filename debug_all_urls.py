#!/usr/bin/env python3
"""
Debug script to test updated URLs.
"""
import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URLS_TO_TEST = {
    "harvard_seas": "https://seas.harvard.edu/people?role=Faculty",
    "uchicago_chemistry": "https://chemistry.uchicago.edu/research/physical",
    "caltech_cce": "https://www.cce.caltech.edu/faculty",
    "northwestern_chemistry": "https://chemistry.northwestern.edu/people/faculty/index.html",
    "yale_chemistry": "https://chem.yale.edu/people/faculty",
}

def test_url(name, url):
    logger.info(f"--- {name} ---")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
        r = requests.get(url, headers=headers, timeout=15)
        logger.info(f"Status: {r.status_code}")
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            logger.info(f"Title: {soup.title.string if soup.title else 'N/A'}")
            
            # Count profile links
            profile_links = soup.find_all('a', href=lambda x: x and ('/people/' in x or '/faculty/' in x or '/profile/' in x or '/directory/' in x) if x else False)
            logger.info(f"Profile-like links: {len(profile_links)}")
            
            # Count "Professor"
            prof_count = len(soup.find_all(string=lambda t: t and 'Professor' in t))
            logger.info(f"'Professor' mentions: {prof_count}")
            
            # Sample names from links
            if profile_links:
                names = [a.get_text(strip=True) for a in profile_links[:5] if a.get_text(strip=True) and len(a.get_text(strip=True)) > 3]
                logger.info(f"Sample names: {names[:3]}")
        else:
            logger.info(f"FAILED: {r.status_code}")
    except Exception as e:
        logger.error(f"Exception: {e}")

if __name__ == "__main__":
    for name, url in URLS_TO_TEST.items():
        test_url(name, url)
        print()
