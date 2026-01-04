#!/usr/bin/env python3
"""
Browser-based scraper for JavaScript-heavy university faculty pages.
Uses Selenium with Chrome in headless mode.
"""

import logging
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


class BrowserScraper:
    """Browser-based scraper using Selenium for JS-rendered pages."""
    
    def __init__(self):
        self.driver = None
        self._setup_driver()
    
    def _setup_driver(self):
        """Initialize Chrome driver in headless mode."""
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            logger.info("Browser driver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize browser driver: {e}")
            self.driver = None
    
    def close(self):
        """Close the browser driver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def _is_valid_name(self, name: str) -> bool:
        """Check if name looks like a valid person name."""
        if not name or len(name) < 4 or len(name) > 60:
            return False
        
        # Skip common non-name patterns
        skip_patterns = ['faculty', 'professor', 'department', 'university', 
                        'contact', 'email', 'phone', 'research', 'home',
                        'about', 'news', 'events', 'more', 'view', 'read',
                        'learn', 'click', 'here', 'page', 'site']
        name_lower = name.lower()
        if any(pattern in name_lower for pattern in skip_patterns):
            return False
        
        # Name should have at least 2 parts (first and last name)
        parts = name.split()
        if len(parts) < 2:
            return False
        
        # Name should contain letters
        if not any(c.isalpha() for c in name):
            return False
        
        return True
    
    def scrape_harvard_seas(self) -> List[Dict]:
        """Scrape Harvard SEAS faculty page using browser."""
        logger.info("Browser scraping Harvard SEAS...")
        
        if not self.driver:
            logger.error("Browser driver not available")
            return []
        
        # Correct URL with Faculty role filter applied
        url = "https://seas.harvard.edu/about-us/directory?role[46]=46"
        faculty_list = []
        
        try:
            self.driver.get(url)
            time.sleep(5)  # Wait for dynamic content
            
            # Wait for links to appear
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/people/']"))
            )
            
            # Scroll to load more content
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Find all profile links
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/people/']")
            
            for link in links:
                try:
                    name = link.text.strip()
                    href = link.get_attribute('href')
                    
                    if not self._is_valid_name(name):
                        continue
                    
                    # Try to get title from parent element
                    title = "Professor"
                    try:
                        parent = link.find_element(By.XPATH, "./ancestor::div[1]")
                        parent_text = parent.text
                        for line in parent_text.split('\n'):
                            if 'Professor' in line or 'Research' in line:
                                title = line.strip()
                                break
                    except:
                        pass
                    
                    faculty_list.append({
                        'name': name,
                        'title': title,
                        'profile_url': href or url,
                        'department_source': url
                    })
                except Exception:
                    continue
            
        except TimeoutException:
            logger.error("Timeout waiting for Harvard SEAS page to load")
        except Exception as e:
            logger.error(f"Error scraping Harvard SEAS: {e}")
        
        # Deduplicate
        seen = set()
        unique = []
        for f in faculty_list:
            key = f['name'].lower()
            if key not in seen:
                seen.add(key)
                unique.append(f)
        
        logger.info(f"Found {len(unique)} faculty from Harvard SEAS (browser)")
        return unique
    
    def scrape_uchicago_chemistry(self) -> List[Dict]:
        """Scrape UChicago Chemistry faculty page using browser."""
        logger.info("Browser scraping UChicago Chemistry...")
        
        if not self.driver:
            logger.error("Browser driver not available")
            return []
        
        url = "https://chemistry.uchicago.edu/faculty"
        faculty_list = []
        
        try:
            self.driver.get(url)
            # Wait for faculty grid items to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href^='/faculty/']"))
            )
            time.sleep(2)
            
            # Use JavaScript to scroll and load all content
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # Find all faculty links
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='/faculty/']")
            
            for link in links:
                try:
                    name = link.text.strip()
                    href = link.get_attribute('href')
                    
                    if not self._is_valid_name(name):
                        continue
                    
                    # Try to get title from grid item container
                    title = "Professor"
                    try:
                        parent = link.find_element(By.XPATH, "./ancestor::div[contains(@class, 'c-alphalist__grid-item')]")
                        parent_text = parent.text
                        if "Title:" in parent_text:
                            for line in parent_text.split('\n'):
                                if "Title:" in line:
                                    title = line.replace("Title:", "").strip()
                                    break
                        elif "Professor" in parent_text:
                            for line in parent_text.split('\n'):
                                if "Professor" in line and len(line) < 80:
                                    title = line.strip()
                                    break
                    except:
                        pass
                    
                    faculty_list.append({
                        'name': name,
                        'title': title,
                        'profile_url': href or url,
                        'department_source': url
                    })
                except Exception:
                    continue
            
        except TimeoutException:
            logger.error("Timeout waiting for UChicago Chemistry page to load")
        except Exception as e:
            logger.error(f"Error scraping UChicago Chemistry: {e}")
        
        # Deduplicate
        seen = set()
        unique = []
        for f in faculty_list:
            key = f['name'].lower()
            if key not in seen:
                seen.add(key)
                unique.append(f)
        
        logger.info(f"Found {len(unique)} faculty from UChicago Chemistry (browser)")
        return unique
    
    def scrape_northwestern_chemistry(self) -> List[Dict]:
        """Scrape Northwestern Chemistry faculty page using browser."""
        logger.info("Browser scraping Northwestern Chemistry...")
        
        if not self.driver:
            logger.error("Browser driver not available")
            return []
        
        url = "https://chemistry.northwestern.edu/people/faculty/index.html"
        faculty_list = []
        
        try:
            self.driver.get(url)
            # Wait for article elements to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "article.people"))
            )
            time.sleep(2)
            
            # Scroll to load all content
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # Find all article.people elements
            articles = self.driver.find_elements(By.CSS_SELECTOR, "article.people")
            
            for article in articles:
                try:
                    # Get name from h3 a
                    name_elem = article.find_element(By.CSS_SELECTOR, "h3 a")
                    name = name_elem.text.strip()
                    href = name_elem.get_attribute('href')
                    
                    if not self._is_valid_name(name):
                        continue
                    
                    # Get title from the article text
                    title = "Professor"
                    article_text = article.text
                    for line in article_text.split('\n'):
                        if 'Professor' in line and len(line) < 80:
                            title = line.strip()
                            break
                    
                    faculty_list.append({
                        'name': name,
                        'title': title,
                        'profile_url': href or url,
                        'department_source': url
                    })
                except Exception:
                    continue
            
        except TimeoutException:
            logger.error("Timeout waiting for Northwestern Chemistry page to load")
        except Exception as e:
            logger.error(f"Error scraping Northwestern Chemistry: {e}")
        
        # Deduplicate
        seen = set()
        unique = []
        for f in faculty_list:
            key = f['name'].lower()
            if key not in seen:
                seen.add(key)
                unique.append(f)
        
        logger.info(f"Found {len(unique)} faculty from Northwestern Chemistry (browser)")
        return unique
    
    def scrape_caltech_cce(self) -> List[Dict]:
        """Scrape Caltech CCE faculty page using browser."""
        logger.info("Browser scraping Caltech CCE...")
        
        if not self.driver:
            logger.error("Browser driver not available")
            return []
        
        url = "https://www.cce.caltech.edu/faculty"
        faculty_list = []
        
        try:
            self.driver.get(url)
            # Wait for content to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/people/']"))
            )
            time.sleep(2)
            
            # Find all people links
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/people/']")
            
            for link in links:
                try:
                    name = link.text.strip()
                    href = link.get_attribute('href')
                    
                    if not self._is_valid_name(name):
                        continue
                    
                    title = "Professor"
                    try:
                        parent = link.find_element(By.XPATH, "./..")
                        parent_text = parent.text
                        for line in parent_text.split('\n'):
                            if "Professor" in line and len(line) < 100:
                                title = line.strip()
                                break
                    except:
                        pass
                    
                    faculty_list.append({
                        'name': name,
                        'title': title,
                        'profile_url': href or url,
                        'department_source': url
                    })
                except Exception:
                    continue
            
        except TimeoutException:
            logger.error("Timeout waiting for Caltech CCE page to load")
        except Exception as e:
            logger.error(f"Error scraping Caltech CCE: {e}")
        
        # Deduplicate
        seen = set()
        unique = []
        for f in faculty_list:
            key = f['name'].lower()
            if key not in seen:
                seen.add(key)
                unique.append(f)
        
        logger.info(f"Found {len(unique)} faculty from Caltech CCE (browser)")
        return unique
    
    def scrape_all(self) -> List[Dict]:
        """Scrape all JS-heavy university pages."""
        all_faculty = []
        
        all_faculty.extend(self.scrape_harvard_seas())
        all_faculty.extend(self.scrape_uchicago_chemistry())
        all_faculty.extend(self.scrape_northwestern_chemistry())
        all_faculty.extend(self.scrape_caltech_cce())
        
        self.close()
        return all_faculty


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    scraper = BrowserScraper()
    faculty = scraper.scrape_all()
    
    print(f"\nTotal faculty found: {len(faculty)}")
    for f in faculty[:10]:
        print(f"  - {f['name']} ({f['title']})")
