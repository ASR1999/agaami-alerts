import os
import time
import datetime
import requests
import feedparser
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
model = genai.GenerativeModel('gemini-2.0-flash')

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
    
    # --- IMPROVED PROMPT STRUCTURE ---
    prompt = f"""
    ### SYSTEM ROLE
    You are an Expert Legal Data Analyst and Research Assistant. Your objective is to extract structured metadata from legal news articles and blog posts with high precision.

    ### OUTPUT FORMAT REQUIRMENT
    You must return strictly a SINGLE line of text separated by pipes (|) in the following order:
    Author Name|Contact Info|Article Title|Date|Summary

    ### DATA EXTRACTION RULES
    1. **Author Name**: 
       - Extract the specific name of the journalist or contributor. 
       - If no individual author is named, use the Publication/Website Name (e.g., "LiveLaw News Network").
       - Do not use generic terms like "Staff" or "Admin".

    2. **Contact Info**: 
       - Aggressively search for the author's Email, Twitter/X handle, or LinkedIn URL mentioned in the text.
       - If the specific author's contact is missing, look for the publication's "Editor" or "Contact Us" email address in the footer/header text.
       - If absolutely no contact info is found, output: "Not Found".

    3. **Article Title**: 
       - Extract the exact headline of the article.

    4. **Date**: 
       - STRICTLY convert all dates to **DD-MM-YYYY** format.
       - Logic for Relative Dates: 
         - If text says "2 hours ago" or "Today", use today's date: {current_date}
         - If text says "Yesterday", calculate the date for yesterday.
         - If text says "Updated: Feb 14", assume the current year 2026.

    5. **Summary**: 
       - Write a high-quality, professional summary in exactly 2 sentences.
       - Focus strictly on the *legal, judicial, or policy* implications of the article.
       - Do not start with "The article discusses..." or "This text says...". Start directly with the subject (e.g., "The Supreme Court ruled that...").

    ### NEGATIVE CONSTRAINTS (CRITICAL)
    - **NO HEADER ROW**: Do not output "Author|Contact|Title..." at the start.
    - **NO MARKDOWN**: Do not use bold (**), italics (*), or code blocks (```).
    - **NO CONVERSATION**: Do not include "Here is the data" or "I have extracted...". Output ONLY the pipe-separated line.
    - **JUNK DETECTION**: If the text provided is a Login Page, Subscription Paywall, Robot Check, or Cookie Notice, output ONLY the single word: SKIP

    ### INPUT TEXT START
    {text_content[:15000]} 
    ### INPUT TEXT END
    """
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            if "429" in str(e):
                print(f"  !! Rate limit. Waiting 65s... ({attempt+1}/{max_retries})")
                time.sleep(65)
            else:
                return "Error|Error|Error|Error|Error"
    return "Error|Quota Exceeded|Error|Error|Error"

def scrape_and_process(url):
    # 1. HARD BLOCKLIST
    blocked_domains = ["youtube.com", "youtu.be", "pressreader.com", "msn.com"]
    if any(domain in url for domain in blocked_domains):
        print(f"  -- Skipping blocked domain: {url}")
        return None

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove junk elements
        for script in soup(["script", "style", "nav", "footer", "form", "iframe", "header"]):
            script.extract()
            
        text = soup.get_text(separator=' ', strip=True)
        
        # AI Extraction
        ai_data = extract_info_with_ai(text, url)
        
        # FILTER: Handle "SKIP" command from AI
        if "SKIP" in ai_data:
            print("  -- AI indicated junk content (Login/Captcha). Skipping.")
            return None

        # Clean up any accidental newlines
        lines = ai_data.split('\n')
        clean_line = lines[-1] 
        
        row_data = clean_line.split('|')
        
        # Ensure 5 columns
        if len(row_data) < 5:
            row_data += ["Error"] * (5 - len(row_data))
            
        # 2. PYTHON TRAP: Kill Ghost Headers
        first_col = row_data[0].strip().lower()
        if "author name" in first_col or "author" == first_col:
            print("  -- Caught a ghost header row. Discarding.")
            return None
            
        # 3. PYTHON TRAP: Kill Empty/Junk Rows
        if "not found" in row_data[0].lower() and "not found" in row_data[2].lower():
             print("  -- Caught a junk row (Not Found). Discarding.")
             return None

        # Add Source URL and Time
        row_data.append(url)
        row_data.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        return row_data
        
    except Exception as e:
        print(f"Scraping Error {url}: {e}")
        return None

def save_to_sheet(rows):
    SCOPES = ['[https://www.googleapis.com/auth/spreadsheets](https://www.googleapis.com/auth/spreadsheets)']
    creds = Credentials.from_service_account_info(
        eval(GOOGLE_CREDS_JSON), scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=creds)
    
    body = {'values': rows}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

if __name__ == "__main__":
    all_new_rows = []
    processed_urls = set()

    print("Starting Daily Run...")

    for feed_url in RSS_FEEDS:
        print(f"Checking Feed: {feed_url}...")
        try:
            feed = feedparser.parse(feed_url)
            
            # Process Top 2 items
            for entry in feed.entries[:2]:
                raw_link = entry.link
                clean_link = clean_google_url(raw_link)
                
                if clean_link in processed_urls:
                    continue
                processed_urls.add(clean_link)

                print(f"  > Processing: {clean_link}")
                data = scrape_and_process(clean_link)
                
                if data:
                    all_new_rows.append(data)
                    time.sleep(15) 
        except Exception as e:
            print(f"Error parsing feed {feed_url}: {e}")

    if all_new_rows:
        print(f"Saving {len(all_new_rows)} rows to sheets...")
        save_to_sheet(all_new_rows)
    else:
        print("No data collected.")
