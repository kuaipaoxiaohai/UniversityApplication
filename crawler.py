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
import pandas as pd
import json
import time
import random
import re
import logging
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
    "stanford_cheme": "https://cheme.stanford.edu/people/faculty",
    "stanford_mse": "https://mse.stanford.edu/people/faculty",
    "stanford_doerr": "https://sustainability.stanford.edu/our-community/faculty-0",
    "mit_dmse": "https://dmse.mit.edu/people/faculty/"
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
    
    def polite_request(self, url: str, timeout: int = 10) -> Optional[requests.Response]:
        """
        Make a polite HTTP request with random delay (1-3 seconds).
        
        Args:
            url: The URL to request
            timeout: Request timeout in seconds
            
        Returns:
            Response object or None if request failed
        """
        time.sleep(random.uniform(1, 3))
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
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
        
        soup = BeautifulSoup(response.text, 'lxml')
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
            
            soup = BeautifulSoup(response.text, 'lxml')
            
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
        
        soup = BeautifulSoup(response.text, 'lxml')
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
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Check if redirected to profiles.stanford.edu
        final_url = response.url
        
        # Try to find Stanford Profiles link
        stanford_profile_link = soup.find('a', href=lambda x: x and 'profiles.stanford.edu' in x if x else False)
        
        if stanford_profile_link and 'profiles.stanford.edu' not in final_url:
            # Visit the Stanford Profiles page for more complete info
            profiles_url = stanford_profile_link.get('href')
            profiles_response = self.polite_request(profiles_url)
            if profiles_response:
                soup = BeautifulSoup(profiles_response.text, 'lxml')
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
        
        soup = BeautifulSoup(response.text, 'lxml')
        
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
            else:
                profile_info = {}
            
            # Merge data
            complete_faculty = {**faculty, **profile_info}
            self.faculty_data.append(complete_faculty)
        
        logger.info(f"Stage 2 complete: Scraped {len(self.faculty_data)} faculty profiles")
    
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
        
        # For duplicates, merge and keep most complete
        deduplicated = []
        
        for name, entries in name_to_entries.items():
            if len(entries) == 1:
                deduplicated.append(entries[0])
            else:
                # Merge entries
                merged = entries[0].copy()
                
                # Collect all department sources
                all_sources = [e.get('department_source', '') for e in entries]
                merged['department_sources'] = list(set(filter(None, all_sources)))
                
                # Take the most complete data for each field
                for entry in entries[1:]:
                    for key, value in entry.items():
                        if key == 'department_source':
                            continue
                        if value and not merged.get(key):
                            merged[key] = value
                        elif isinstance(value, list) and len(value) > len(merged.get(key, [])):
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
        
        # Flatten data for CSV
        flat_data = []
        for faculty in self.faculty_data:
            flat = faculty.copy()
            
            # Convert lists to strings
            if 'top_publications' in flat:
                flat['top_publications'] = ' | '.join(flat['top_publications'])
            if 'department_sources' in flat:
                flat['department_sources'] = ', '.join(flat['department_sources'])
            
            flat_data.append(flat)
        
        df = pd.DataFrame(flat_data)
        
        # Reorder columns
        preferred_order = [
            'name', 'title', 'department_source', 'department_sources',
            'email', 'phone', 'assistant_email', 
            'profile_url', 'lab_website', 'google_scholar',
            'top_publications'
        ]
        
        columns = [c for c in preferred_order if c in df.columns]
        columns += [c for c in df.columns if c not in columns]
        df = df[columns]
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"CSV saved: {filename}")
    
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
