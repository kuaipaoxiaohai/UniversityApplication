import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_url(url, name):
    logger.info(f"Testing {name}: {url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Test headers
        title = soup.title.string if soup.title else "No Title"
        logger.info(f"Page Title: {title}")
        
        # Check specific selectors
        if "harvard" in name:
            target = soup.find(string="Department Chair")
            if target:
                logger.info("Found 'Department Chair' text.")
                parent = target.find_parent('div')
                while parent:
                    logger.info(f"Parent class: {parent.get('class')}")
                    if parent.get('class') and 'views-row' in parent.get('class'):
                         logger.info("Found views-row ancestor!")
                         break
                    parent = parent.find_parent('div')
            else:
                 logger.info("'Department Chair' not found. Dumping first 5000 chars of body.")
                 print(soup.body.prettify()[:5000])
                 
        if "home" in name:
             links = soup.find_all('a', href=True)
             for link in links:
                 text = link.get_text(strip=True).lower()
                 href = link['href']
                 if 'faculty' in text or 'people' in text:
                     logger.info(f"Found navigation link: {text} -> {href}")
        
        elif "yale_dump" in name:
            print("Dumping Yale Body Start:")
            print(soup.body.prettify()[:5000])
                    
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")

if __name__ == "__main__":
    urls = [
        ("princeton_final", "https://chemistry.princeton.edu/faculty-research/"),
        ("uchicago_final", "https://chemistry.uchicago.edu/faculty"),
    ]
    
    for name, url in urls:
        logger.info(f"--- Analyzing {name} ---")
        try:
            # Use Googlebot UA
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                logger.info(f"{name} 200 OK (Googlebot)")
                soup = BeautifulSoup(r.text, 'html.parser')
                
                # Check specific headings and text
                logger.info(f"Page Title: {soup.title.string if soup.title else 'No Title'}")
                
                # Check for specific names found in search
                check_names = ["Bocarsly", "Galli", "Anderson", "Cava"] 
                found_names = [n for n in check_names if n in r.text]
                logger.info(f"Found known names: {found_names}")

                logger.info("First 500 chars of text:")
                logger.info(soup.get_text()[:500].replace('\n', ' '))


            else:
                logger.info(f"{name} Failed: {r.status_code}")
        except Exception as e:
            logger.info(f"{name} Exception: {e}")
