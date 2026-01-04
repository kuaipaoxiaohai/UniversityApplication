#!/usr/bin/env python3
"""
Script to run browser scraper and merge data with faculty_data.json
"""

import json
import logging
from browser_scraper import BrowserScraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load existing data
    try:
        with open('faculty_data.json', 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = []
    
    logger.info(f"Loaded {len(existing_data)} existing faculty entries")
    
    # Create index by name for quick lookup
    existing_names = {entry['name'].lower() for entry in existing_data}
    
    # Run browser scraper
    scraper = BrowserScraper()
    new_faculty = scraper.scrape_all()
    
    logger.info(f"Browser scraper found {len(new_faculty)} faculty")
    
    # Track counts by source
    added_by_source = {}
    
    # Add new faculty data
    added_count = 0
    for faculty in new_faculty:
        name_key = faculty['name'].lower()
        if name_key not in existing_names:
            # Add default fields to match existing format
            full_entry = {
                'name': faculty['name'],
                'title': faculty['title'],
                'profile_url': faculty['profile_url'],
                'department_source': faculty['department_source'],
                'email': '',
                'phone': '',
                'lab_website': '',
                'google_scholar': '',
                'top_publications': [],
                'assistant_email': '',
                'research_interests': [],
                'department_sources': [faculty['department_source']]
            }
            existing_data.append(full_entry)
            existing_names.add(name_key)
            added_count += 1
            
            # Track by source
            source = faculty['department_source']
            if source not in added_by_source:
                added_by_source[source] = 0
            added_by_source[source] += 1
    
    # Save updated data
    with open('faculty_data.json', 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Added {added_count} new faculty entries")
    logger.info(f"Total faculty in database: {len(existing_data)}")
    
    for source, count in added_by_source.items():
        logger.info(f"  - {source}: {count} new")

if __name__ == "__main__":
    main()
