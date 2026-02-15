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

# Your list of RSS Feeds
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

# Initialize Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

def clean_google_url(google_link):
    """Extracts the real URL from the Google Alert redirect."""
    try:
        parsed = urlparse(google_link)
        query_params = parse_qs(parsed.query)
        if 'url' in query_params:
            return query_params['url'][0]
        return google_link
    except:
        return google_link

def extract_info_with_ai(text_content, url):
    prompt = f"""
    You are a legal research assistant. Analyze this text from {url}.
    
    TEXT:
    {text_content[:12000]} 

    TASK:
    Extract these details. If not found, write "Not Found".
    1. Author Name
    2. Author Contact (Look for email, Twitter, LinkedIn, or affiliation)
    3. Article Title
    4. Date of Publishing
    5. Summary (Concise, 2-3 sentences)

    OUTPUT FORMAT (Strictly Pipe Separated):
    Author Name|Contact Info|Title|Date|Summary
    """
    
    # Retry Logic for 429 Errors
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            if "429" in str(e):
                wait_time = 65  # Wait 65 seconds to clear quota
                print(f"  !! Speed limit hit (429). Cooling down for {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"  !! AI Error: {e}")
                return "Error|Error|Error|Error|Error"
    
    return "Error|Quota Exceeded|Error|Error|Error"

def scrape_and_process(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Cleanup
        for script in soup(["script", "style", "nav", "footer"]):
            script.extract()
            
        text = soup.get_text(separator=' ', strip=True)
        
        # AI Extraction
        ai_data = extract_info_with_ai(text, url)
        
        row_data = ai_data.split('|')
        
        # Ensure 5 columns from AI
        if len(row_data) < 5:
            row_data += ["Error"] * (5 - len(row_data))
            
        # Add Source URL
        row_data.append(url)
        
        # Add Run Timestamp (Current Time)
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_data.append(current_time)
        
        return row_data
        
    except Exception as e:
        print(f"Scraping Error {url}: {e}")
        return None

def save_to_sheet(rows):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
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
            
            # Process top 2 items
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
                    # Sleep 15s to be safe
                    time.sleep(15) 
        except Exception as e:
            print(f"Error parsing feed {feed_url}: {e}")

    if all_new_rows:
        print(f"Saving {len(all_new_rows)} rows to sheets...")
        save_to_sheet(all_new_rows)
    else:
        print("No data collected.")
