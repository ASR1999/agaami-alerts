import os
import time
import json
import datetime
import requests
import feedparser
import re
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import google.generativeai as genai
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- CONFIGURATION ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

RSS_FEEDS = [
    "https://www.google.co.in/alerts/feeds/15296787733172383910/8375788598715294266",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/509298250493646222",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/1826406499160227986",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/509298250493645067",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/1826406499160231435",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/509298250493647621",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/8375788598715294733",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/1826406499160231824",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/2743587427347210023",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/1826406499160228592",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/2743587427347208743",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/14596142030123297166",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/1747714098892080080",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/2743587427347208849",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/14596142030123298194",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/1747714098892082418",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/2743587427347206826",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/2743587427347209845",
    "https://www.google.co.in/alerts/feeds/15296787733172383910/2743587427347208696"
]

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash', generation_config={"response_mime_type": "application/json"})

def get_google_sheets_service():
    """Authenticates and returns the Sheets service."""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(
        eval(GOOGLE_CREDS_JSON), scopes=SCOPES
    )
    return build('sheets', 'v4', credentials=creds)

def get_existing_urls(service):
    """Reads Column F (Source URL) to check for duplicates."""
    try:
        # UPDATED: Read from F2 to the end (F2:F) to skip the Header row
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Sheet1!F2:F"
        ).execute()
        
        rows = result.get('values', [])
        
        # UPDATED: Robust filtering
        # 1. Checks if row is not empty
        # 2. Checks if the cell value is not just whitespace
        existing_set = set()
        for row in rows:
            if row and len(row) > 0 and row[0].strip():
                existing_set.add(row[0].strip())
                
        return existing_set
    except Exception as e:
        print(f"Error reading existing URLs: {e}")
        return set()

def clean_google_url(google_link):
    try:
        parsed = urlparse(google_link)
        query_params = parse_qs(parsed.query)
        if 'url' in query_params:
            return query_params['url'][0]
        return google_link
    except:
        return google_link

def extract_info_with_ai(text_content, url):
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    prompt = f"""
    ### SYSTEM ROLE

    You are an Expert Legal Data Analyst and Research Assistant. Your objective is to extract structured metadata from legal news articles and blog posts with high precision.

    ### OUTPUT FORMAT REQUIREMENT

    You must return a valid JSON object with the following specific keys:
    {{
        "author": "Name of journalist or publication",
        "contact": "Email, Twitter handle, or LinkedIn URL (or 'Not Found')",
        "title": "Exact headline",
        "date": "DD-MM-YYYY",
        "summary": "2-sentence legal summary",
        "is_junk": false  // Set to true if content is login/paywall/junk
    }}

    ### DATA EXTRACTION RULES

    1. **Author Name** (Key: "author"): 
       - Extract the specific name of the journalist or contributor. 
       - If no individual author is named, use the Publication/Website Name (e.g., "LiveLaw News Network").
       - Do not use generic terms like "Staff" or "Admin".

    2. **Contact Info** (Key: "contact"): 
       - Aggressively search for the author's Email, Twitter/X handle, or LinkedIn URL mentioned in the text.
       - If the specific author's contact is missing, look for the publication's "Editor" or "Contact Us" email address in the footer/header text.
       - If absolutely no contact info is found, output: "Not Found".

    3. **Article Title** (Key: "title"): 
       - Extract the exact headline of the article.

    4. **Date** (Key: "date"): 
       - STRICTLY convert all dates to **DD-MM-YYYY** format.
       - Logic for Relative Dates: 
         - If text says "2 hours ago" or "Today", use today's date: {current_date}
         - If text says "Yesterday", calculate the date for yesterday.
         - If text says "Updated: Feb 14", assume the current year 2026.

    5. **Summary** (Key: "summary"): 
       - Write a high-quality, professional summary in exactly 2 sentences.
       - Focus strictly on the *legal, judicial, or policy* implications of the article.
       - Do not start with "The article discusses..." or "This text says...". Start directly with the subject (e.g., "The Supreme Court ruled that...").

    6. **Junk Detection** (Key: "is_junk"):
       - If the text provided is a Login Page, Subscription Paywall, Robot Check, or Cookie Notice, set "is_junk" to true and leave other fields null or empty. Otherwise, set "is_junk" to false.

    ### NEGATIVE CONSTRAINTS (CRITICAL)
    - **NO MARKDOWN**: Do not include markdown formatting like ```json ... ```.
    - **NO CONVERSATION**: Do not include "Here is the JSON" or intro text. Output ONLY the JSON object.

    ### INPUT TEXT START

    {text_content[:15000]} 

    ### INPUT TEXT END
    """
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            data = json.loads(response.text)
            
            # --- SAFETY CHECK: Handle List vs Dict ---
            if isinstance(data, list):
                if len(data) > 0 and isinstance(data[0], dict):
                    return data[0] # Take first item if it's a list
                else:
                    return None # Discard empty lists or lists of strings
            
            if not isinstance(data, dict):
                return None # Discard plain strings
                
            return data
            
        except Exception as e:
            if "429" in str(e):
                print(f"  !! Rate limit. Waiting 65s... ({attempt+1}/{max_retries})")
                time.sleep(65)
            else:
                print(f"  !! JSON/AI Error: {e}")
                return None
    return None

def scrape_and_process(url):
    blocked_domains = ["youtube.com", "youtu.be", "pressreader.com", "msn.com"]
    if any(domain in url for domain in blocked_domains):
        print(f"  -- Skipping blocked domain: {url}")
        return None

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        for script in soup(["script", "style", "nav", "footer", "form", "iframe", "header"]):
            script.extract()
            
        text = soup.get_text(separator=' ', strip=True)
        
        data = extract_info_with_ai(text, url)
        
        if not data:
            return None

        if data.get("is_junk", False):
            print("  -- AI identified junk content. Skipping.")
            return None
        
        if data.get("author") == "Not Found" and data.get("title") == "Not Found":
             print("  -- Empty data returned. Skipping.")
             return None

        # Format: Author | Contact | Title | Date | Summary | URL | RunTime
        row_data = [
            data.get("author", "N/A"),
            data.get("contact", "N/A"),
            data.get("title", "N/A"),
            data.get("date", "N/A"),
            data.get("summary", "N/A"),
            url,
            # Programmatically adding the current run time here
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
        
        return row_data
        
    except Exception as e:
        print(f"Scraping Error {url}: {e}")
        return None

def save_to_sheet(service, rows):
    body = {'values': rows}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

if __name__ == "__main__":
    all_new_rows = []
    
    print("Authenticating with Google Sheets...")
    sheets_service = get_google_sheets_service()
    
    print("Fetching existing URLs to avoid duplicates...")
    existing_urls = get_existing_urls(sheets_service)
    print(f"Found {len(existing_urls)} existing articles in the sheet.")

    print("Starting Daily Run...")

    for feed_url in RSS_FEEDS:
        print(f"Checking Feed: {feed_url}...")
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:2]:
                raw_link = entry.link
                clean_link = clean_google_url(raw_link)
                
                # DUPLICATE CHECK
                if clean_link in existing_urls:
                    print(f"  > Skipping Duplicate: {clean_link}")
                    continue
                
                # Add to temporary set so we don't process same link twice in one run
                existing_urls.add(clean_link) 

                print(f"  > Processing: {clean_link}")
                data = scrape_and_process(clean_link)
                
                if data:
                    all_new_rows.append(data)
                    time.sleep(15) 
        except Exception as e:
            print(f"Error parsing feed {feed_url}: {e}")

    if all_new_rows:
        print(f"Saving {len(all_new_rows)} rows to sheets...")
        save_to_sheet(sheets_service, all_new_rows)
    else:
        print("No new data collected.")
