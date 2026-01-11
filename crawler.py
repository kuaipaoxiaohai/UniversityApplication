#!/usr/bin/env python3
"""
Academic Faculty Data Crawler for Stanford and MIT
===================================================

A Python web crawler to extract structured faculty data (contact info, 
research interests, publications) from Stanford ChemE, MSE, Doerr School, 
and MIT DMSE for undergraduate applicant outreach.

Usage:
    python crawler.py

Output:
    - faculty_data.csv: CSV format for easy reading
    - faculty_data.json: Complete structured data in JSON format
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import random
import re
import logging
import os
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Title filtering configuration
INCLUDE_TITLES = [
    "Professor",
    "Assistant Professor", 
    "Associate Professor",
    "Department Chair",
    "Department Head"
]

EXCLUDE_KEYWORDS = [
    "Lecturer",
    "Adjunct",
    "Instructor", 
    "Staff",
    "Emeritus",
    "Visiting",
    "By Courtesy",
    "Courtesy",
    "Professor of the Practice"  # MIT has this title
]

# Invalid name patterns to exclude (navigation links, section headers)
INVALID_NAMES = [
    "courtesy appointments",
    "emeritus",
    "emerita",
    "lecturer",
    "lecturers",
    "staff",
    "faculty in memoriam",
    "in memoriam",
    "visiting faculty",
    "adjunct",
    "by courtesy",
    "graduate students",
    "postdocs",
    "research scientists",
    "administrative",
    "incoming",
    "faculty",
    "people",
    "all",
    "view"
]

# Target URLs
TARGET_URLS = {
    # Stanford & MIT (existing)
    "stanford_cheme": "https://cheme.stanford.edu/people/faculty",
    "stanford_mse": "https://mse.stanford.edu/people/faculty",
    "stanford_doerr": "https://sustainability.stanford.edu/our-community/faculty-0",
    "mit_dmse": "https://dmse.mit.edu/people/faculty/",
    
    # Harvard
    "harvard_chemistry": "https://chemistry.harvard.edu/people",
    "harvard_seas": "https://seas.harvard.edu/people?role=Faculty",
    
    # Yale
    "yale_chemistry": "https://chem.yale.edu/people/faculty",
    
    # Princeton
    "princeton_chemistry": "https://chemistry.princeton.edu/faculty-research/",
    
    # University of Chicago - Use physical chemistry page which lists faculty
    "uchicago_chemistry": "https://chemistry.uchicago.edu/research/physical",
    
    # Northwestern
    "northwestern_chemistry": "https://chemistry.northwestern.edu/people/faculty/index.html",
    "northwestern_mse": "https://www.mccormick.northwestern.edu/materials-science/people/faculty/",
    
    # UC Berkeley
    "berkeley_chemistry": "https://chemistry.berkeley.edu/people/faculty",
    "berkeley_cbe": "https://chemistry.berkeley.edu/people/cbe-faculty",
    
    # Caltech - use faculty subdirectory
    "caltech_cce": "https://www.cce.caltech.edu/faculty",
    "caltech_materials": "https://www.cms.caltech.edu/people"
}


class FacultyCrawler:
    """
    Web crawler for extracting faculty data from Stanford and MIT.
    
    The crawler operates in two stages:
    1. Stage 1: Manifest Generation - Extract faculty lists from target URLs
    2. Stage 2: Deep Scraping - Visit each profile page for detailed info
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        self.faculty_manifest: List[Dict] = []
        self.faculty_data: List[Dict] = []
        
        # Load existing data if available
        self.load_existing_data()
    
    def load_existing_data(self):
        """Load existing faculty data to prevent data loss."""
        try:
            if os.path.exists('faculty_data.json'):
                with open('faculty_data.json', 'r', encoding='utf-8') as f:
                    self.faculty_data = json.load(f)
                logger.info(f"Loaded {len(self.faculty_data)} existing faculty entries.")
        except Exception as e:
            logger.error(f"Error loading existing data: {e}")
    
    def polite_request(self, url: str, timeout: int = 10, headers: Dict = None) -> Optional[requests.Response]:
        """
        Make a polite HTTP request with random delay (1-3 seconds).
        
        Args:
            url: The URL to request
            timeout: Request timeout in seconds
            headers: Optional headers to override/merge with session headers
            
        Returns:
            Response object or None if request failed
        """
        time.sleep(random.uniform(1, 3))
        
        try:
            # Prepare arguments
            kwargs = {'timeout': timeout}
            if headers:
                kwargs['headers'] = headers
                
            response = self.session.get(url, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def is_valid_name(self, name: str) -> bool:
        """
        Check if the name is a valid person name (not a navigation link or section header).
        
        Args:
            name: The name to check
            
        Returns:
            True if it looks like a valid person name
        """
        if not name:
            return False
        
        name_lower = name.lower().strip()
        
        # Must be at least 3 characters
        if len(name_lower) < 3:
            return False
        
        # Check against invalid names list
        for invalid in INVALID_NAMES:
            if name_lower == invalid or name_lower.startswith(invalid + ' '):
                return False
        
        # Valid names typically have at least 2 words (first and last name)
        # But some names like "Yi Cui" are valid
        words = name_lower.split()
        if len(words) < 2:
            # Single word is suspicious unless it's short (like a Chinese name)
            if len(words) == 1 and len(words[0]) > 15:
                return False
        
        # Check if name contains suspicious patterns
        suspicious = ['http', 'www', '.com', '.edu', 'click', 'more', 'view all']
        for susp in suspicious:
            if susp in name_lower:
                return False
        
        return True
    
    def is_valid_professor_title(self, title: str) -> bool:
        """
        Check if the title indicates a valid professor (not excluded).
        
        Args:
            title: The academic title to check
            
        Returns:
            True if title should be included, False otherwise
        """
        if not title:
            return False
            
        title_lower = title.lower()
        
        # First check exclusions
        for exclude in EXCLUDE_KEYWORDS:
            if exclude.lower() in title_lower:
                return False
        
        # Then check inclusions
        for include in INCLUDE_TITLES:
            if include.lower() in title_lower:
                return True
        
        return False
    
    # ==================== Stage 1: Faculty List Scraping ====================
    
    def scrape_stanford_department(self, url: str, department_name: str) -> List[Dict]:
        """
        Scrape Stanford ChemE or MSE faculty page.
        
        Args:
            url: Faculty listing page URL
            department_name: Name of the department for logging
            
        Returns:
            List of faculty dictionaries with name, title, profile_url
        """
        logger.info(f"Scraping {department_name} faculty list...")
        
        response = self.polite_request(url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Stanford department pages group faculty under h2 headers by title
        # Look for faculty cards/items
        
        # Try to find faculty containers - these sites use various structures
        faculty_cards = soup.find_all('article', class_=lambda x: x and 'person' in x.lower() if x else False)
        
        if not faculty_cards:
            # Alternative: look for links within faculty sections
            people_section = soup.find('div', class_='view-people') or soup.find('main')
            if people_section:
                # Find all person links
                person_links = people_section.find_all('a', href=lambda x: x and '/people/' in x if x else False)
                
                # Get the current section title (Professor, Associate Professor, etc.)
                current_title = "Professor"  # Default
                
                for link in person_links:
                    name = link.get_text(strip=True)
                    if not name or len(name) < 2:
                        continue
                    
                    # Skip navigation links
                    if name.lower() in ['faculty', 'people', 'all', 'view']:
                        continue
                    
                    profile_url = urljoin(url, link.get('href', ''))
                    
                    # Try to find the title from nearby elements or parent section
                    parent = link.find_parent(['div', 'section', 'article'])
                    title_elem = None
                    if parent:
                        title_elem = parent.find(['h2', 'h3', 'span'], class_=lambda x: x and 'title' in x.lower() if x else False)
                        if not title_elem:
                            title_elem = parent.find(['p', 'span', 'div'], class_=lambda x: x and ('subtitle' in x.lower() or 'role' in x.lower()) if x else False)
                    
                    # Look backwards for section header
                    prev = link.find_previous(['h2', 'h3'])
                    if prev:
                        prev_text = prev.get_text(strip=True)
                        if any(t.lower() in prev_text.lower() for t in ['Professor', 'Chair']):
                            current_title = prev_text
                    
                    faculty_list.append({
                        'name': name,
                        'title': title_elem.get_text(strip=True) if title_elem else current_title,
                        'profile_url': profile_url,
                        'department_source': url
                    })
        
        # Remove duplicates based on name
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            if f['name'].lower() not in seen_names:
                seen_names.add(f['name'].lower())
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from {department_name}")
        return unique_faculty
    
    def scrape_stanford_doerr(self) -> List[Dict]:
        """
        Scrape Stanford Doerr School of Sustainability faculty page.
        This page has a large list and may require pagination handling.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping Stanford Doerr School faculty list...")
        
        url = TARGET_URLS["stanford_doerr"]
        all_faculty = []
        page = 0
        
        while True:
            page_url = f"{url}?page={page}" if page > 0 else url
            response = self.polite_request(page_url)
            
            if not response:
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            page_faculty = []
            
            # The Doerr School page lists faculty as links in a specific section
            # Look for the main content area with faculty listings
            main_content = soup.find('main') or soup.find('div', class_='main-content') or soup
            
            # Faculty are typically listed as links with their names
            # Look for links that contain faculty profile URLs
            faculty_links = main_content.find_all('a', href=lambda x: x and (
                '/person/' in x or 
                '/people/' in x or 
                '/faculty/' in x or
                'profiles.stanford.edu' in x
            ) if x else False)
            
            for link in faculty_links:
                name = link.get_text(strip=True)
                
                # Skip invalid names
                if not self.is_valid_name(name):
                    continue
                
                # Get profile URL
                profile_url = urljoin(url, link.get('href', ''))
                
                # Try to find title from nearby elements
                parent = link.find_parent(['li', 'div', 'article', 'td'])
                title = "Professor"  # Default
                
                if parent:
                    # Look for title in sibling or child elements
                    title_elem = parent.find(['span', 'p', 'div'], class_=lambda x: x and ('title' in x.lower() or 'position' in x.lower()) if x else False)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                
                page_faculty.append({
                    'name': name,
                    'title': title,
                    'profile_url': profile_url,
                    'department_source': url
                })
            
            if not page_faculty:
                # No faculty found with links, try alternative approach
                # Look for text that appears to be faculty names (First Last format)
                logger.warning("No faculty profile links found, trying text-based extraction...")
                
                # Find view-content or similar containers
                content_divs = main_content.find_all(['div', 'section'], class_=lambda x: x and ('view' in x.lower() or 'content' in x.lower() or 'listing' in x.lower()) if x else False)
                
                for div in content_divs:
                    # Look for links within
                    for link in div.find_all('a', href=True):
                        name = link.get_text(strip=True)
                        href = link.get('href', '')
                        
                        # Check if this looks like a faculty link
                        if self.is_valid_name(name) and ('stanford.edu' in href or href.startswith('/')):
                            page_faculty.append({
                                'name': name,
                                'title': 'Professor',
                                'profile_url': urljoin(url, href),
                                'department_source': url
                            })
                
                if page == 0 and not page_faculty:
                    break
            
            all_faculty.extend(page_faculty)
            
            # Check for next page
            next_link = soup.find('a', {'rel': 'next'}) or soup.find('a', string=re.compile(r'next|›|»', re.I))
            if not next_link:
                break
            
            page += 1
            if page > 10:  # Safety limit
                logger.warning("Reached pagination limit")
                break
        
        # Remove duplicates and filter invalid names
        seen_names = set()
        unique_faculty = []
        for f in all_faculty:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from Doerr School")
        return unique_faculty
    
    def scrape_mit_dmse(self) -> List[Dict]:
        """
        Scrape MIT DMSE faculty page.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping MIT DMSE faculty list...")
        
        url = TARGET_URLS["mit_dmse"]
        response = self.polite_request(url)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # MIT DMSE has faculty listed in a grid/list format
        # Look for faculty items/cards
        
        # Find all links to faculty profiles
        all_links = soup.find_all('a', href=lambda x: x and '/people/faculty/' in x if x else False)
        
        for link in all_links:
            href = link.get('href', '')
            
            # Skip if it's the main faculty page
            if href.rstrip('/').endswith('/people/faculty'):
                continue
            
            # Get the name - might be in the link text or a child element
            name = link.get_text(strip=True)
            
            # Clean up name - remove title suffixes if present
            name_lines = name.split('\n')
            if name_lines:
                name = name_lines[0].strip()
            
            if not name or len(name) < 2 or name.lower() in ['faculty', 'emeritus', 'visiting']:
                continue
            
            # Check for title in link or parent
            title = ""
            parent = link.find_parent(['li', 'div', 'article'])
            if parent:
                # Title might be in a separate element
                title_text = parent.get_text()
                for t in ['Professor', 'Associate Professor', 'Assistant Professor', 'Department Head']:
                    if t in title_text:
                        title = t
                        break
            
            if not title:
                # Default based on section
                title = "Professor"
            
            profile_url = urljoin(url, href)
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and len(name_key) > 2:
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from MIT DMSE")
        return unique_faculty
    
    def scrape_harvard_chemistry(self) -> List[Dict]:
        """
        Scrape Harvard Chemistry and Chemical Biology faculty page.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping Harvard Chemistry faculty list...")
        
        url = TARGET_URLS["harvard_chemistry"]
        response = self.polite_request(url)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Harvard CCB lists all people, need to filter for faculty
        # New selector based on search-page__result-items container
        people_container = soup.find('div', class_='search-page__result-items')
        
        people_items = []
        if people_container:
            # Iterate over direct children or find all cards
            # The cards seem to be div.search-page__result-items > div
            people_items = people_container.find_all('div', recursive=False)
            if not people_items:
                 # Fallback deep search
                 people_items = people_container.find_all('div', class_='page-card__text')
        else:
            # Fallback to old selectors just in case
            people_items = soup.find_all(['div', 'li'], class_=lambda x: x and ('person' in x.lower() or 'views-row' in x.lower()) if x else False)

        for item in people_items:
            # Name extraction
            name_elem = item.find(['h2', 'h3', 'h4', 'div'], class_=lambda x: x and ('title' in x.lower() or 'name' in x.lower() or 'heading' in x.lower()) if x else False)
            if not name_elem:
                 name_elem = item.find('a')
            
            # Additional check for Harvard specific class
            if not name_elem:
                name_elem = item.find(class_='page-card__title')
            
            if not name_elem:
                continue

            name = name_elem.get_text(strip=True)
            href = name_elem.get('href', '')
            if not href and name_elem.name == 'a':
                href = name_elem['href']
            elif not href:
                 link = item.find('a', href=True)
                 href = link.get('href', '') if link else ''
            
            # Check title to ensure it's a professor/faculty
            title = ""
            title_elem = item.find(['div', 'p'], class_=lambda x: x and ('field-name-field-person-title' in x or 'title' in x or 'job-title' in x) if x else False)
            
            # Specific Harvard class from debug
            if not title_elem:
                 title_elem = item.find(class_='field--name-field-hwp-person-prof-title')

            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Filter for Professor titles but also accept empty titles if likely faculty (weak check)
            # Better to be strict: must have "Professor" or "Faculty"
            if not title:
                title = "Professor"

            if "Professor" not in title and "Faculty" not in title and "Lecturer" not in title and "Chair" not in title:
                 continue
            
            if not self.is_valid_name(name):
                continue
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from Harvard Chemistry")
        return unique_faculty
    
    def scrape_harvard_seas(self) -> List[Dict]:
        """
        Scrape Harvard SEAS faculty page.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping Harvard SEAS faculty list...")
        
        url = TARGET_URLS["harvard_seas"]
        response = self.polite_request(url)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # SEAS has a people directory with faculty cards
        faculty_cards = soup.find_all(['article', 'div'], class_=lambda x: x and ('person' in x.lower() or 'faculty' in x.lower() or 'card' in x.lower()) if x else False)
        
        if not faculty_cards:
            faculty_cards = soup.find_all('a', href=lambda x: x and '/people/' in x if x else False)
        
        for card in faculty_cards:
            if card.name == 'a':
                name = card.get_text(strip=True)
                href = card.get('href', '')
            else:
                name_elem = card.find(['h2', 'h3', 'h4', 'a'])
                name = name_elem.get_text(strip=True) if name_elem else ''
                link = card.find('a', href=True)
                href = link.get('href', '') if link else ''
            
            if not self.is_valid_name(name):
                continue
            
            title = "Professor"
            title_elem = card.find(['p', 'span'], class_=lambda x: x and 'title' in x.lower() if x else False)
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from Harvard SEAS")
        return unique_faculty
    
    def scrape_yale_chemistry(self) -> List[Dict]:
        """
        Scrape Yale Chemistry faculty page.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping Yale Chemistry faculty list...")
        
        url = TARGET_URLS["yale_chemistry"]
        response = self.polite_request(url)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Yale has 53 profile-like links - extract faculty from these
        profile_links = soup.find_all('a', href=lambda x: x and ('/people/' in x or '/faculty/' in x) if x else False)
        
        for link in profile_links:
            name = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Skip navigation/category links
            if not name or len(name) < 3 or name.lower() in ['faculty', 'people', 'primary faculty', 'emeriti', 'lecturers', 'secondary appointments']:
                continue
                
            if not self.is_valid_name(name):
                continue
            
            # Try to get title from parent or sibling elements
            title = "Professor"
            parent = link.find_parent(['div', 'article', 'li'])
            if parent:
                title_elem = parent.find(['p', 'span', 'div'], class_=lambda x: x and ('title' in x.lower() or 'position' in x.lower()) if x else False)
                if title_elem:
                    title = title_elem.get_text(strip=True)
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from Yale Chemistry")
        return unique_faculty
    
    def scrape_princeton_chemistry(self) -> List[Dict]:
        """
        Scrape Princeton Chemistry faculty page.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping Princeton Chemistry faculty list...")
        
        url = TARGET_URLS["princeton_chemistry"]
        # Use Googlebot UA to ensure static HTML is returned
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
        response = self.polite_request(url, headers=headers)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Princeton has 124 profile-like links - extract faculty from these
        profile_links = soup.find_all('a', href=lambda x: x and ('/people/' in x or '/faculty/' in x) if x else False)
        
        for link in profile_links:
            name = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Skip navigation/category links and short names
            if not name or len(name) < 4:
                continue
            
            # Skip common navigation text
            skip_texts = ['faculty', 'people', 'research', 'home', 'about', 'contact', 'news']
            if name.lower() in skip_texts:
                continue
                
            if not self.is_valid_name(name):
                continue
            
            # Try to get title from parent elements
            title = "Professor"
            parent = link.find_parent(['div', 'article', 'li'])
            if parent:
                # Look for title in parent text
                parent_text = parent.get_text(separator='|')
                for part in parent_text.split('|'):
                    if 'Professor' in part and len(part) < 60:
                        title = part.strip()
                        break
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from Princeton Chemistry")
        return unique_faculty
    
    def scrape_uchicago_chemistry(self) -> List[Dict]:
        """
        Scrape University of Chicago Chemistry faculty page.
        
        Returns:
            List of faculty dictionaries
        """
        logger.info("Scraping UChicago Chemistry faculty list...")
        
        url = TARGET_URLS["uchicago_chemistry"]
        # Use Googlebot UA
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
        response = self.polite_request(url, headers=headers)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Find all links to faculty profiles (relative paths often contain /directory/)
        profile_links = soup.find_all('a', href=lambda x: x and ('/directory/' in x or '/people/' in x or '/profile/' in x) if x else False)
        
        if not profile_links:
            # Fallback: Find links with faculty-like names visible in page
            profile_links = soup.find_all('a', href=True)
        
        for link in profile_links:
            name = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Skip short text or navigation links
            if not name or len(name) < 4 or len(name) > 50:
                continue
            
            # Skip common navigation text
            skip_texts = ['faculty', 'people', 'research', 'home', 'about', 'contact', 'news', 'read more', 'learn more']
            if name.lower() in skip_texts or any(skip in name.lower() for skip in skip_texts):
                continue
                
            if not self.is_valid_name(name):
                continue
            
            # Get title from parent text if available
            title = "Professor"
            parent = link.find_parent(['div', 'p', 'li'])
            if parent:
                parent_text = parent.get_text(separator='|')
                for part in parent_text.split('|'):
                    if 'Professor' in part and len(part) < 80:
                        title = part.strip()
                        break
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from UChicago Chemistry")
        return unique_faculty
    
    def scrape_northwestern_department(self, url: str, department_name: str) -> List[Dict]:
        """
        Scrape Northwestern department faculty page.
        
        Args:
            url: Faculty listing page URL
            department_name: Name for logging
            
        Returns:
            List of faculty dictionaries
        """
        logger.info(f"Scraping {department_name} faculty list...")
        
        # Use Googlebot UA
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
        response = self.polite_request(url, headers=headers)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Northwestern has 54 "Professor" mentions - find elements containing these
        # Look for all text nodes containing "Professor" and extract faculty from parents
        prof_elements = soup.find_all(string=lambda t: t and 'Professor' in t and len(t) < 100)
        
        seen_parents = set()
        for prof_text in prof_elements:
            # Find containing element
            parent = prof_text.find_parent(['div', 'article', 'li', 'tr'])
            if not parent or parent in seen_parents:
                continue
            seen_parents.add(parent)
            
            # Find name - usually in a heading or strong link
            name_elem = parent.find(['h2', 'h3', 'h4', 'strong', 'a'])
            if not name_elem:
                continue
            
            name = name_elem.get_text(strip=True)
            if not self.is_valid_name(name):
                continue
            
            # Get profile link
            href = ''
            link = parent.find('a', href=True)
            if link:
                href = link.get('href', '')
            
            # Get title from text
            title = str(prof_text).strip() if prof_text else "Professor"
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from {department_name}")
        return unique_faculty
    
    def scrape_berkeley_department(self, url: str, department_name: str) -> List[Dict]:
        """
        Scrape UC Berkeley College of Chemistry faculty page.
        
        Args:
            url: Faculty listing page URL
            department_name: Name for logging
            
        Returns:
            List of faculty dictionaries
        """
        logger.info(f"Scraping {department_name} faculty list...")
        
        # Use Googlebot UA
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
        response = self.polite_request(url, headers=headers)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Berkeley has 30 profile-like links per department - extract from these
        profile_links = soup.find_all('a', href=lambda x: x and ('/people/' in x or '/faculty/' in x or '/profile/' in x) if x else False)
        
        for link in profile_links:
            name = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Skip navigation links and short names
            if not name or len(name) < 4:
                continue
            
            # Skip common navigation text
            skip_texts = ['faculty', 'people', 'chemistry faculty', 'cbe faculty', 'meet the', 'research', 'home']
            if any(skip in name.lower() for skip in skip_texts):
                continue
                
            if not self.is_valid_name(name):
                continue
            
            # Try to get title from parent elements
            title = "Professor"
            parent = link.find_parent(['div', 'article', 'li'])
            if parent:
                # Look for Professor in parent text
                parent_text = parent.get_text(separator='|')
                for part in parent_text.split('|'):
                    if 'Professor' in part and len(part) < 60:
                        title = part.strip()
                        break
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from {department_name}")
        return unique_faculty
    
    def scrape_caltech_department(self, url: str, department_name: str) -> List[Dict]:
        """
        Scrape Caltech department faculty page.
        
        Args:
            url: Faculty listing page URL
            department_name: Name for logging
            
        Returns:
            List of faculty dictionaries
        """
        logger.info(f"Scraping {department_name} faculty list...")
        
        response = self.polite_request(url)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        faculty_list = []
        
        # Caltech faculty listings
        faculty_items = soup.find_all(['article', 'div', 'li'], class_=lambda x: x and ('faculty' in x.lower() or 'person' in x.lower() or 'profile' in x.lower() or 'card' in x.lower()) if x else False)
        
        if not faculty_items:
            faculty_items = soup.find_all('a', href=lambda x: x and ('/people/' in x or '/directory/' in x) if x else False)
        
        for item in faculty_items:
            if item.name == 'a':
                name = item.get_text(strip=True)
                href = item.get('href', '')
            else:
                name_elem = item.find(['h2', 'h3', 'h4', 'a'])
                name = name_elem.get_text(strip=True) if name_elem else ''
                link = item.find('a', href=True)
                href = link.get('href', '') if link else ''
            
            if not self.is_valid_name(name):
                continue
            
            title = "Professor"
            title_elem = item.find(['p', 'span', 'div'], class_=lambda x: x and ('title' in x.lower() or 'position' in x.lower()) if x else False)
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            profile_url = urljoin(url, href) if href else url
            
            faculty_list.append({
                'name': name,
                'title': title,
                'profile_url': profile_url,
                'department_source': url
            })
        
        # Remove duplicates
        seen_names = set()
        unique_faculty = []
        for f in faculty_list:
            name_key = f['name'].lower().strip()
            if name_key not in seen_names and self.is_valid_name(f['name']):
                seen_names.add(name_key)
                unique_faculty.append(f)
        
        logger.info(f"Found {len(unique_faculty)} faculty from {department_name}")
        return unique_faculty
    
    def run_stage1(self) -> List[Dict]:
        """
        Run Stage 1: Collect faculty manifests from all target URLs.
        
        Returns:
            List of all faculty entries (unfiltered)
        """
        logger.info("=" * 50)
        logger.info("Starting Stage 1: Faculty Manifest Generation")
        logger.info("=" * 50)
        
        all_faculty = []
        
        # Stanford ChemE
        cheme_faculty = self.scrape_stanford_department(
            TARGET_URLS["stanford_cheme"], 
            "Stanford Chemical Engineering"
        )
        all_faculty.extend(cheme_faculty)
        
        # Stanford MSE
        mse_faculty = self.scrape_stanford_department(
            TARGET_URLS["stanford_mse"],
            "Stanford Materials Science & Engineering"
        )
        all_faculty.extend(mse_faculty)
        
        # Stanford Doerr School
        doerr_faculty = self.scrape_stanford_doerr()
        all_faculty.extend(doerr_faculty)
        
        # MIT DMSE
        mit_faculty = self.scrape_mit_dmse()
        all_faculty.extend(mit_faculty)
        
        # ==================== New Universities ====================
        
        # Harvard
        harvard_chem_faculty = self.scrape_harvard_chemistry()
        all_faculty.extend(harvard_chem_faculty)
        
        harvard_seas_faculty = self.scrape_harvard_seas()
        all_faculty.extend(harvard_seas_faculty)
        
        # Yale
        yale_faculty = self.scrape_yale_chemistry()
        all_faculty.extend(yale_faculty)
        
        # Princeton
        princeton_faculty = self.scrape_princeton_chemistry()
        all_faculty.extend(princeton_faculty)
        
        # UChicago
        uchicago_faculty = self.scrape_uchicago_chemistry()
        all_faculty.extend(uchicago_faculty)
        
        # Northwestern
        northwestern_chem = self.scrape_northwestern_department(
            TARGET_URLS["northwestern_chemistry"],
            "Northwestern Chemistry"
        )
        all_faculty.extend(northwestern_chem)
        
        northwestern_mse = self.scrape_northwestern_department(
            TARGET_URLS["northwestern_mse"],
            "Northwestern Materials Science"
        )
        all_faculty.extend(northwestern_mse)
        
        # UC Berkeley
        berkeley_chem = self.scrape_berkeley_department(
            TARGET_URLS["berkeley_chemistry"],
            "UC Berkeley Chemistry"
        )
        all_faculty.extend(berkeley_chem)
        
        berkeley_cbe = self.scrape_berkeley_department(
            TARGET_URLS["berkeley_cbe"],
            "UC Berkeley Chemical & Biomolecular Engineering"
        )
        all_faculty.extend(berkeley_cbe)
        
        # Caltech
        caltech_cce = self.scrape_caltech_department(
            TARGET_URLS["caltech_cce"],
            "Caltech Chemistry & Chemical Engineering"
        )
        all_faculty.extend(caltech_cce)
        
        caltech_materials = self.scrape_caltech_department(
            TARGET_URLS["caltech_materials"],
            "Caltech Materials Science"
        )
        all_faculty.extend(caltech_materials)
        
        logger.info(f"Stage 1 complete: Found {len(all_faculty)} total faculty entries")
        
        # Filter faculty based on titles AND valid names
        filtered_faculty = [
            f for f in all_faculty 
            if self.is_valid_professor_title(f['title']) and self.is_valid_name(f['name'])
        ]
        logger.info(f"After filtering: {len(filtered_faculty)} faculty with valid professor titles and names")
        
        self.faculty_manifest = filtered_faculty
        return filtered_faculty
    
    # ==================== Stage 2: Deep Profile Scraping ====================
    
    def extract_email(self, soup: BeautifulSoup) -> str:
        """
        Extract email from a page, handling obfuscation.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Email address or empty string if not found
        """
        # Look for mailto links
        mailto_links = soup.find_all('a', href=lambda x: x and 'mailto:' in x if x else False)
        for link in mailto_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            if '@' in email:
                return email.split('?')[0]  # Remove query params
        
        # Look for email patterns in text
        email_patterns = [
            r'[\w\.-]+@[\w\.-]+\.\w+',
            r'[\w\.-]+\s*\[at\]\s*[\w\.-]+\s*\[dot\]\s*\w+',
            r'[\w\.-]+\s*\(at\)\s*[\w\.-]+\s*\(dot\)\s*\w+'
        ]
        
        page_text = soup.get_text()
        for pattern in email_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                email = match.group()
                # Clean up obfuscation
                email = re.sub(r'\s*\[at\]\s*', '@', email, flags=re.I)
                email = re.sub(r'\s*\(at\)\s*', '@', email, flags=re.I)
                email = re.sub(r'\s*\[dot\]\s*', '.', email, flags=re.I)
                email = re.sub(r'\s*\(dot\)\s*', '.', email, flags=re.I)
                return email
        
        return ""
    
    def extract_phone(self, soup: BeautifulSoup) -> str:
        """
        Extract phone number from a page.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Phone number or empty string
        """
        # Look for tel: links
        tel_links = soup.find_all('a', href=lambda x: x and 'tel:' in x if x else False)
        for link in tel_links:
            phone = link.get('href', '').replace('tel:', '').strip()
            if phone:
                return phone
        
        # Look for phone patterns in text
        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        page_text = soup.get_text()
        match = re.search(phone_pattern, page_text)
        if match:
            return match.group()
        
        return ""
    
    def extract_lab_website(self, soup: BeautifulSoup, base_url: str) -> str:
        """
        Extract lab/research group website URL.
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative links
            
        Returns:
            Lab website URL or empty string
        """
        # Look for links with lab/research/group keywords
        keywords = ['lab', 'research', 'group', 'website']
        
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True).lower()
            href = link.get('href', '')
            
            # Check if link text contains keywords
            if any(kw in link_text for kw in keywords):
                # Skip internal links and social media
                if any(skip in href.lower() for skip in ['linkedin', 'twitter', 'facebook', 'youtube', 'instagram']):
                    continue
                return urljoin(base_url, href)
        
        # Also check for "Web page" or personal website links
        web_link = soup.find('a', text=re.compile(r'web\s*page|personal|homepage', re.I))
        if web_link and web_link.get('href'):
            return urljoin(base_url, web_link.get('href'))
        
        return ""
    
    def extract_google_scholar(self, soup: BeautifulSoup) -> str:
        """
        Extract Google Scholar profile URL.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Google Scholar URL or empty string
        """
        scholar_link = soup.find('a', href=lambda x: x and 'scholar.google' in x if x else False)
        if scholar_link:
            return scholar_link.get('href', '')
        return ""
    
    def extract_publications(self, soup: BeautifulSoup) -> List[str]:
        """
        Extract first 3-5 publication titles.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of publication titles
        """
        publications = []
        
        # Look for publication sections
        pub_section = soup.find(['section', 'div'], class_=lambda x: x and 'publication' in x.lower() if x else False)
        if not pub_section:
            pub_section = soup.find(['h2', 'h3'], text=re.compile(r'publication|paper|research', re.I))
            if pub_section:
                pub_section = pub_section.find_parent(['section', 'div'])
        
        if pub_section:
            # Look for publication items
            pub_items = pub_section.find_all(['li', 'article', 'div'], class_=lambda x: x and ('item' in x.lower() or 'pub' in x.lower()) if x else False)
            if not pub_items:
                pub_items = pub_section.find_all('li')
            
            for item in pub_items[:5]:
                title = item.get_text(strip=True)
                # Clean up - take first line or first sentence
                title = title.split('\n')[0].strip()
                if len(title) > 10 and len(title) < 500:  # Reasonable length
                    publications.append(title[:300])  # Truncate if too long
        
        return publications[:5]
    
    def extract_assistant_email(self, soup: BeautifulSoup) -> str:
        """
        Extract administrative assistant/contact email.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Assistant email or empty string
        """
        # Look for "Administrative Contact" section
        admin_section = soup.find(text=re.compile(r'administrative\s*contact|assistant|coordinator', re.I))
        if admin_section:
            parent = admin_section.find_parent(['div', 'li', 'section'])
            if parent:
                mailto = parent.find('a', href=lambda x: x and 'mailto:' in x if x else False)
                if mailto:
                    return mailto.get('href', '').replace('mailto:', '').split('?')[0]
        
        return ""
    
    def extract_research_interests(self, soup: BeautifulSoup, profile_url: str = '') -> List[str]:
        """
        Extract research interests/areas from a faculty profile.
        
        Args:
            soup: BeautifulSoup object
            profile_url: URL of the profile (to detect site type)
            
        Returns:
            List of research interest keywords
        """
        interests = []
        
        # Stanford Profiles specific extraction
        if 'stanford.edu' in profile_url:
            # Look for "Research & Scholarship" or "Research Interests" section
            research_section = None
            
            # Try finding by section header
            for header_text in ['Research & Scholarship', 'Research Interests', 'Research Focus', 
                               'Areas of Expertise', 'Research Areas', 'Expertise']:
                header = soup.find(['h2', 'h3', 'h4'], string=re.compile(header_text, re.I))
                if header:
                    research_section = header.find_parent(['section', 'div'])
                    break
            
            if research_section:
                # Get text from paragraphs or list items
                for elem in research_section.find_all(['p', 'li', 'span']):
                    text = elem.get_text(strip=True)
                    # Filter out navigation and generic text
                    if text and 10 < len(text) < 200:
                        if not any(skip in text.lower() for skip in ['click', 'view', 'page', 'profile', 'contact', 'email']):
                            interests.append(text)
            
            # Also look for "Bio" section keywords
            if not interests:
                bio_section = soup.find(['div', 'section'], class_=lambda x: x and 'bio' in x.lower() if x else False)
                if bio_section:
                    bio_text = bio_section.get_text()
                    # Extract key research terms from bio
                    keywords = self._extract_keywords_from_text(bio_text)
                    interests.extend(keywords[:5])
        
        # MIT DMSE specific extraction
        elif 'mit.edu' in profile_url:
            # Look for research description or areas
            for class_name in ['research', 'bio', 'description', 'about']:
                section = soup.find(['div', 'section', 'article'], 
                                   class_=lambda x: x and class_name in x.lower() if x else False)
                if section:
                    for p in section.find_all('p'):
                        text = p.get_text(strip=True)
                        if text and 20 < len(text) < 300:
                            keywords = self._extract_keywords_from_text(text)
                            interests.extend(keywords)
                    break
        
        # Generic extraction for other sites
        if not interests:
            # Look for explicit research interest sections
            for pattern in [r'research\s*interest', r'research\s*area', r'expertise', r'specialization']:
                header = soup.find(['h2', 'h3', 'h4', 'strong', 'b'], string=re.compile(pattern, re.I))
                if header:
                    parent = header.find_parent(['div', 'section', 'li'])
                    if parent:
                        for item in parent.find_all(['li', 'p', 'span']):
                            text = item.get_text(strip=True)
                            if 5 < len(text) < 100 and text != header.get_text(strip=True):
                                interests.append(text)
                        if interests:
                            break
        
        # Clean and deduplicate
        cleaned = []
        seen = set()
        for interest in interests:
            # Remove generic/navigation text
            if any(skip in interest.lower() for skip in [
                'stanford profile', 'official site', 'postdoc', 'student', 
                'click here', 'learn more', 'read more', 'view all',
                'contact', 'email', 'phone', 'is part of'
            ]):
                continue
            
            # Normalize
            interest = interest.strip()
            if interest.lower() not in seen and 3 < len(interest) < 100:
                seen.add(interest.lower())
                cleaned.append(interest)
        
        return cleaned[:5]  # Limit to 5 interests
    
    def _extract_keywords_from_text(self, text: str) -> List[str]:
        """
        Extract research keywords from a text block.
        
        Args:
            text: Text to analyze
            
        Returns:
            List of extracted keywords
        """
        keywords = []
        
        # Common research field keywords to look for
        field_keywords = [
            # Materials
            'nanomaterials', 'biomaterials', 'polymers', 'ceramics', 'semiconductors',
            'thin films', 'nanostructures', 'composites', 'alloys', 'surfaces',
            # Chemistry
            'catalysis', 'electrochemistry', 'organic synthesis', 'photochemistry',
            'biochemistry', 'thermodynamics', 'kinetics', 'spectroscopy',
            # Energy
            'solar cells', 'batteries', 'fuel cells', 'photovoltaics', 'energy storage',
            'renewable energy', 'hydrogen', 'carbon capture',
            # Biology
            'drug delivery', 'tissue engineering', 'bioengineering', 'biotechnology',
            'proteins', 'cells', 'molecular biology', 'synthetic biology',
            # Environment
            'climate change', 'sustainability', 'environmental', 'ecology', 
            'water treatment', 'pollution', 'carbon',
            # Physics/Engineering
            'optics', 'photonics', 'electronics', 'transport', 'mechanics',
            'fluid dynamics', 'heat transfer', 'computational',
            # Methods
            'machine learning', 'simulation', 'modeling', 'characterization',
            'microscopy', 'imaging', 'spectroscopy'
        ]
        
        text_lower = text.lower()
        for keyword in field_keywords:
            if keyword in text_lower:
                # Capitalize properly
                keywords.append(keyword.title())
        
        return keywords[:5]
    
    def scrape_stanford_profile(self, profile_url: str) -> Dict:
        """
        Scrape detailed info from a Stanford faculty profile.
        
        Args:
            profile_url: URL to the faculty's profile page
            
        Returns:
            Dictionary with detailed faculty info
        """
        profile_data = {
            'email': '',
            'phone': '',
            'lab_website': '',
            'google_scholar': '',
            'top_publications': [],
            'assistant_email': '',
            'research_interests': []
        }
        
        response = self.polite_request(profile_url)
        if not response:
            return profile_data
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check if redirected to profiles.stanford.edu
        final_url = response.url
        
        # Try to find Stanford Profiles link
        stanford_profile_link = soup.find('a', href=lambda x: x and 'profiles.stanford.edu' in x if x else False)
        
        if stanford_profile_link and 'profiles.stanford.edu' not in final_url:
            # Visit the Stanford Profiles page for more complete info
            profiles_url = stanford_profile_link.get('href')
            profiles_response = self.polite_request(profiles_url)
            if profiles_response:
                soup = BeautifulSoup(profiles_response.text, 'html.parser')
                final_url = profiles_response.url
        
        # Extract all information
        profile_data['email'] = self.extract_email(soup)
        profile_data['phone'] = self.extract_phone(soup)
        profile_data['lab_website'] = self.extract_lab_website(soup, final_url)
        profile_data['google_scholar'] = self.extract_google_scholar(soup)
        profile_data['top_publications'] = self.extract_publications(soup)
        profile_data['assistant_email'] = self.extract_assistant_email(soup)
        profile_data['research_interests'] = self.extract_research_interests(soup, final_url)
        
        return profile_data
    
    def scrape_mit_profile(self, profile_url: str) -> Dict:
        """
        Scrape detailed info from an MIT DMSE faculty profile.
        
        Args:
            profile_url: URL to the faculty's profile page
            
        Returns:
            Dictionary with detailed faculty info
        """
        profile_data = {
            'email': '',
            'phone': '',
            'lab_website': '',
            'google_scholar': '',
            'top_publications': [],
            'assistant_email': '',
            'research_interests': []
        }
        
        response = self.polite_request(profile_url)
        if not response:
            return profile_data
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract all information
        profile_data['email'] = self.extract_email(soup)
        profile_data['phone'] = self.extract_phone(soup)
        profile_data['lab_website'] = self.extract_lab_website(soup, profile_url)
        profile_data['google_scholar'] = self.extract_google_scholar(soup)
        profile_data['top_publications'] = self.extract_publications(soup)
        profile_data['research_interests'] = self.extract_research_interests(soup, profile_url)
        
        return profile_data
    
    def run_stage2(self):
        """
        Run Stage 2: Deep scrape each faculty profile for detailed info.
        """
        logger.info("=" * 50)
        logger.info("Starting Stage 2: Deep Profile Scraping")
        logger.info("=" * 50)
        
        total = len(self.faculty_manifest)
        
        for i, faculty in enumerate(self.faculty_manifest):
            logger.info(f"Scraping profile {i+1}/{total}: {faculty['name']}")
            
            profile_url = faculty.get('profile_url', '')
            
            if not profile_url:
                self.faculty_data.append(faculty)
                continue
            
            # Determine which scraper to use
            if 'stanford.edu' in profile_url:
                profile_info = self.scrape_stanford_profile(profile_url)
            elif 'mit.edu' in profile_url:
                profile_info = self.scrape_mit_profile(profile_url)
            elif any(u in profile_url for u in ['harvard.edu', 'yale.edu', 'princeton.edu', 'uchicago.edu', 'northwestern.edu', 'berkeley.edu', 'caltech.edu']):
                # Use generic scraper for new universities
                profile_info = self.scrape_generic_profile(profile_url)
            else:
                profile_info = {}
            
            # Merge data
            complete_faculty = {**faculty, **profile_info}
            self.faculty_data.append(complete_faculty)
        
        logger.info(f"Stage 2 complete: Scraped {len(self.faculty_data)} faculty profiles")

    def scrape_generic_profile(self, profile_url: str) -> Dict:
        """
        Generic scraper for faculty profiles from standard university websites.
        
        Args:
            profile_url: URL to the faculty's profile page
            
        Returns:
            Dictionary with detailed faculty info
        """
        profile_data = {
            'email': '',
            'phone': '',
            'lab_website': '',
            'google_scholar': '',
            'top_publications': [],
            'assistant_email': '',
            'research_interests': []
        }
        
        response = self.polite_request(profile_url)
        if not response:
            return profile_data
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract all information using existing extraction methods
        profile_data['email'] = self.extract_email(soup)
        profile_data['phone'] = self.extract_phone(soup)
        # Pass profile_url as base_url
        profile_data['lab_website'] = self.extract_lab_website(soup, profile_url)
        profile_data['google_scholar'] = self.extract_google_scholar(soup)
        profile_data['top_publications'] = self.extract_publications(soup)
        # assistant_email extraction might be specific, but try anyway
        profile_data['assistant_email'] = self.extract_assistant_email(soup)
        profile_data['research_interests'] = self.extract_research_interests(soup, profile_url)
        
        return profile_data
    
    # ==================== Deduplication & Output ====================
    
    def deduplicate(self) -> List[Dict]:
        """
        Remove duplicate faculty entries, keeping the most complete data.
        
        Returns:
            Deduplicated list of faculty
        """
        logger.info("Deduplicating faculty data...")
        
        # Group by normalized name
        name_to_entries = {}
        
        for faculty in self.faculty_data:
            name = faculty.get('name', '').lower().strip()
            if not name:
                continue
                
            if name not in name_to_entries:
                name_to_entries[name] = []
            name_to_entries[name].append(faculty)
            
        deduplicated = []
        for name, entries in name_to_entries.items():
            if len(entries) == 1:
                deduplicated.append(entries[0])
            else:
                # Merge entries to keep the best data
                # Sort entries by "quality" (has email, phone, etc) descending
                def score_entry(e):
                    score = 0
                    if e.get('email'): score += 10
                    if e.get('phone'): score += 5
                    if e.get('top_publications'): score += 3
                    if e.get('research_interests'): score += len(e.get('research_interests', []))
                    if 'stanford.edu' in e.get('email', '') or 'mit.edu' in e.get('email', ''): score += 2
                    return score
                
                sorted_entries = sorted(entries, key=score_entry, reverse=True)
                merged = sorted_entries[0].copy()
                
                # Collect all department sources
                all_sources = []
                for e in entries:
                     src = e.get('department_source', '')
                     if src:
                         all_sources.append(src)
                     if e.get('department_sources'):
                         all_sources.extend(e.get('department_sources'))
                
                merged['department_sources'] = list(set(all_sources))
                
                # If the best entry is missing something that others have, fill it
                for entry in sorted_entries[1:]:
                    for key, value in entry.items():
                        if key in ['department_source', 'department_sources']:
                            continue
                        # If merged is missing val, take from others
                        if value and not merged.get(key):
                             merged[key] = value
                        # If merged has empty list and others have list, take it
                        elif isinstance(value, list) and value and not merged.get(key):
                             merged[key] = value
                
                deduplicated.append(merged)
        
        logger.info(f"After deduplication: {len(deduplicated)} unique faculty")
        self.faculty_data = deduplicated
        return deduplicated
    
    def save_csv(self, filename: str = "faculty_data.csv"):
        """
        Save faculty data to CSV file.
        
        Args:
            filename: Output filename
        """
        logger.info(f"Saving data to {filename}...")
        
        if not self.faculty_data:
            logger.warning("No data to save.")
            return

        # Prepare data for CSV
        flat_data = []
        for faculty in self.faculty_data:
            flat = faculty.copy()
            
            # Convert lists to strings
            if 'top_publications' in flat:
                if isinstance(flat['top_publications'], list):
                    flat['top_publications'] = ' | '.join(flat['top_publications'])
            if 'department_sources' in flat:
                 if isinstance(flat['department_sources'], list):
                    flat['department_sources'] = ', '.join(flat['department_sources'])
            if 'research_interests' in flat:
                if isinstance(flat['research_interests'], list):
                    flat['research_interests'] = ', '.join(flat['research_interests'])
            
            flat_data.append(flat)
        
        # Determine columns
        all_keys = set()
        for item in flat_data:
            all_keys.update(item.keys())
            
        # Reorder columns
        preferred_order = [
            'name', 'title', 'department_source', 'department_sources',
            'email', 'phone', 'assistant_email', 
            'profile_url', 'lab_website', 'google_scholar',
            'top_publications', 'research_interests'
        ]
        
        columns = [c for c in preferred_order if c in all_keys]
        columns += [c for c in all_keys if c not in columns]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(flat_data)
            logger.info(f"CSV saved: {filename}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
    
    def save_json(self, filename: str = "faculty_data.json"):
        """
        Save faculty data to JSON file.
        
        Args:
            filename: Output filename
        """
        logger.info(f"Saving data to {filename}...")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.faculty_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON saved: {filename}")
    
    def run(self):
        """
        Run the complete crawling pipeline.
        """
        logger.info("=" * 60)
        logger.info("Academic Faculty Data Crawler")
        logger.info("=" * 60)
        
        # Stage 1: Collect faculty lists
        self.run_stage1()
        
        # Stage 2: Deep scrape profiles
        self.run_stage2()
        
        # Deduplicate
        self.deduplicate()
        
        # Save outputs
        self.save_csv()
        self.save_json()
        
        logger.info("=" * 60)
        logger.info("Crawling complete!")
        logger.info(f"Total unique faculty: {len(self.faculty_data)}")
        logger.info("Output files: faculty_data.csv, faculty_data.json")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    crawler = FacultyCrawler()
    crawler.run()


if __name__ == "__main__":
    main()
