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
    # UPDATED PROMPT: Stricter formatting rules
    prompt = f"""
    You are a data extraction bot. Analyze this text from {url}.
    
    TEXT START:
    {text_content[:12000]} 
    TEXT END

    TASK:
    Extract the following.
    1. Author Name (If not found, assume the Publication/Website Name)
    2. Author Contact (Email/Twitter? If not found, write "N/A")
    3. Article Title
    4. Date (Convert to DD-MM-YYYY format if possible. If 'x hours ago', calculate the date based on today.)
    5. Summary (Strictly 2 sentences max).

    CRITICAL FORMATTING RULES:
    - Return ONLY the data joined by pipes (|). 
    - DO NOT include a header row.
    - DO NOT use markdown.
    - DO NOT break lines. Keep it on one single line.
    - If the text looks like a login screen, cookie error, or robot check, just output: SKIP

    Example Output:
    John Doe|john@example.com|New Law Passed|15-02-2026|The new bill was passed today. It affects tax laws.
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
    # FILTER 1: Skip YouTube/Video links (They break the scraper)
    if "youtube.com" in url or "youtu.be" in url:
        print("  -- Skipping YouTube Link")
        return None

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove junk elements
        for script in soup(["script", "style", "nav", "footer", "form"]):
            script.extract()
            
        text = soup.get_text(separator=' ', strip=True)
        
        # AI Extraction
        ai_data = extract_info_with_ai(text, url)
        
        # FILTER 2: Handle "SKIP" command from AI
        if "SKIP" in ai_data:
            print("  -- AI indicated junk content (Login/Captcha). Skipping.")
            return None

        # Clean up any accidental newlines or headers the AI might still output
        lines = ai_data.split('\n')
        # Take the last line if multiple are returned (usually the data is last)
        clean_line = lines[-1] 
        
        row_data = clean_line.split('|')
        
        if len(row_data) < 5:
            row_data += ["Error"] * (5 - len(row_data))
            
        row_data.append(url)
        row_data.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
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
            
            # Reduced to Top 2 items per feed
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
