Task: Develop an Academic Faculty Data Crawler for Stanford and MIT
Objective
Create a Python-based web crawler to extract structured data (contact info, research interests, publications) of faculty members from specific departments at Stanford University and MIT. The output will be used by an undergraduate applicant for academic outreach.

Target URLs
Stanford Chemical Engineering: https://cheme.stanford.edu/people/faculty

Stanford Materials Science & Engineering: https://mse.stanford.edu/people/faculty

Stanford Doerr School of Sustainability: https://sustainability.stanford.edu/our-community/faculty-0 (Requires pagination handling).

MIT Materials Science and Engineering (DMSE): https://dmse.mit.edu/people/faculty/

Core Requirements
1. Filtering Logic (Crucial)
Include: Only faculty with titles containing "Professor" (Assistant, Associate, Full).

Exclude: "Lecturer", "Adjunct", "Instructor", "Staff", "Emeritus", "Visiting".

Context: The user is an undergraduate looking for research mentors. Exclude retired or non-research staff.

2. Data Extraction Schema
For each faculty member, extract and store:

name: Full name (string).

title: Academic title (string).

department_source: The list URL they were found on (string).

profile_url: Link to their detailed bio page (string).

email: Email address (string). Handle obfuscation (e.g., "name [at] stanford.edu").

lab_website: URL to their personal group/lab site if found (string).

google_scholar: Link to Google Scholar profile if found (string).

top_publications: A list of the first 3-5 publication titles found on their profile page (List).

3. Technical Strategy
The crawler should operate in two stages:

Stage 1: Manifest Generation
Iterate through the Target URLs.

Parse the HTML list to extract Name, Title, and Profile URL.

Stanford Doerr Note: This site uses pagination. Inspect the HTML for "Next" buttons or URL parameters (e.g., ?page=X) and traverse all pages.

Store this initial list.

Stage 2: Deep Scraping
Visit each profile_url collected in Stage 1.

Stanford Redirects: Be aware that some Stanford links redirect to profiles.stanford.edu. Your parser must detect this domain change and use appropriate selectors for the Profiles layout (where email is often in a sidebar and publications in a tab).

MIT Logic: MIT emails are likely in a "Contact Info" section. Look for a "Key Publications" section for papers or Google Scholar links.

Politeness: Implement a random delay (1-3 seconds) between requests to avoid IP bans.

Error Handling: Use try-except blocks. If a profile fails, log the URL and continue. Do not crash.

4. Output Format
Save the final data as faculty_data.csv (for easy reading) and faculty_data.json (for complete structure).

Deduplication: Before saving, remove duplicate entries based on name. Stanford faculty are often listed in multiple departments (e.g., ChemE and Materials). Keep the entry with the most complete data.

Python Dependencies Recommendation
requests: For HTTP requests.

beautifulsoup4: For HTML parsing.

pandas: For data structuring and export.

re: For regex-based email extraction.

time & random: For rate limiting.

Deliverables
crawler.py: Well-commented, modular Python script.

requirements.txt: List of dependencies.

Instructions on how to run the script.