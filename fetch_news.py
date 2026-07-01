import os
import requests
import json
import time
from datetime import datetime

# Media Distribution Settings
CHANNEL = "CPBN"
CATEGORIES = ["business", "technology", "world"]
# Major high-volatility financial keywords to parse for breaking alerts
MAJOR_KEYWORDS = ["crash", "fed", "acquisition", "rates", "collapse", "billion", "stocks", "breaking", "inflation", "apple", "nvidia", "crypto", "AI", "Cybersecurity", "Semiconductors", "Tariff", "Inflation", "Cryptocurrency", "Budget", "Interest Rate", "Iran", "Israel", "Ukraine", "China", "Taiwan", "US", "Election", "Climate", "Earthquake", "Wildfire", "Flood"]

GNEWS_API_KEY = os.environ.get("NEWS_API_KEY")

def load_existing_database():
    """Loads previous records to prevent wiping historical archives during the cycle"""
    if os.path.exists("news.json"):
        try:
            with open("news.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"en": {}}
    return {"en": {}}

def score_article_importance(title, description):
    """Programmatically checks if an incoming wire headline contains market-moving events"""
    combined_text = f"{title or ''} {description or ''}".lower()
    matches = sum(1 for word in MAJOR_KEYWORDS if word in combined_text)
    return "MAJOR_ALERT" if matches >= 2 else "STANDARD"

def fetch_category_stream(category):
    """Queries news channels securely for active market topics"""
    url = f"https://gnews.io/api/v4/top-headlines?category={category}&lang=en&apikey={GNEWS_API_KEY}"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            return response.json().get("articles", [])
        print(f"⚠️ API Mirror returned status code {response.status_code} for category: [{category.upper()}]")
        return []
    except Exception as e:
        print(f"❌ Failed to establish API handshake link for {category}: {e}")
        return []

def main():
    if not GNEWS_API_KEY:
        print("Aborting compilation sequence: Missing valid operational NEWS_API_KEY secret token.")
        return

    db = load_existing_database()
    
    # Force 'db["en"]' to be a dictionary if it detects an old flat list format
    if "en" not in db or not isinstance(db["en"], dict):
        db["en"] = {}

    total_new_ingested = 0

    for idx, cat in enumerate(CATEGORIES):
        # Introduce a 2-second structural spacing delay between categories to prevent API rate limit rejection
        if idx > 0:
            print("Pacing API connection... holding for 2 seconds.")
            time.sleep(2)

        if cat not in db["en"]:
            db["en"][cat] = []

        print(f"Polling active pipeline stream for Category: [{cat.upper()}]")
        raw_items = fetch_category_stream(cat)
        
        # Build maps of existing tracking URLs to avoid duplicate records
        existing_urls = {item["url"] for item in db["en"][cat]}
        
        fresh_records = []
        for item in raw_items:
            url = item.get("url")
            if not url or url in existing_urls:
                continue
                
            title = item.get("title")
            desc = item.get("description")
            importance = score_article_importance(title, desc)
            
            fresh_records.append({
                "title": title,
                "desc": desc,
                "image": item.get("image") or "https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?q=80&w=800",
                "url": url,
                "source": item.get("source", {}).get("name", "CPBN Wire"),
                "date": item.get("publishedAt") or datetime.utcnow().isoformat(),
                "priority": importance
            })
            existing_urls.add(url)
            total_new_ingested += 1

        # Combine historical archives with newly fetched records (Newest entries land at index 0)
        combined_pool = fresh_records + db["en"][cat]
        
        # Sort strictly by timestamp string metadata to maintain a clean timeline flow
        combined_pool.sort(key=lambda x: x["date"], reverse=True)
        
        # Clip individual category tracks at 70 entries to stay within the 200 aggregate threshold
        db["en"][cat] = combined_pool[:70]

    # Stamp runtime release metrics
    db["lastGlobalSync"] = datetime.utcnow().isoformat() + "Z"
    
    with open("news.json", "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=4)
        
    print(f"Database compile pass complete. Ingested {total_new_ingested} new original files across network tracks.")

if __name__ == "__main__":
    main()
