import sys
import os
import json
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
import re
import time
from datetime import datetime, timezone
from collections import defaultdict
from collections import Counter
import logging
import glob


def strip_html(html_content):
    """Remove HTML tags and extract text."""
    # Remove script and style elements
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Extract links before removing tags
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Extract images
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    # Clean whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text, links, images

def wait_for_complete_file(path, timeout=300, interval=3):

    start = time.time()
    
    while time.time() - start < timeout:
        if os.path.exists(path):
            print(f"Find the completion file: {path}")
            return True

        time.sleep(interval)
    
    raise TimeoutError(f"Timeout")

results = []

def load_html_files(directory):

    for html_file in sorted(glob.glob(os.path.join(directory, "*.html"))):
        try:
            with open(html_file, "r", encoding="utf-8", errors="ignore") as f:
                results.append({
                    "HTML file": html_file,
                    "status": "success"
                })
                yield html_file, f.read() # return (filename, HTML content(str))

        except Exception as e:
            print(f"Failed to load {html_file}: {e}")
            results.append({
                "HTML file": html_file,
                "status": "failed"
            })
            continue

def analyze_text(text: str):

    words = re.findall(r"\w+(?:[-']\w+)*", text, flags=re.UNICODE)
    word_count = len(words)

    total_words_length = sum(len(w) for w in words) 
    avg_words_length = total_words_length / word_count if word_count > 0 else 0

    # Split by ., !, ? 
    sentences = re.split(r"[.!?]", text)
    sentences = [s.strip() for s in sentences if s.strip()]
    sentence_count = len(sentences)

    # Split by 2+ line breaks
    paragraphs = re.split(r"(?:\r?\n){2,}", text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    paragraph_count = len(paragraphs)

    return {
        "words": word_count,
        "sentences": sentence_count,
        "paragraphs": paragraph_count,
        "avg_word_length": avg_words_length
    }


def main():

    wait_for_complete_file("/shared/status/fetch_complete.json")

    raw_dir = "/shared/raw"
    processed_dir = "/shared/processed"
    status_dir = "/shared/status"
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(status_dir, exist_ok=True)

    i = 1
    for html_filename, html_content in load_html_files(raw_dir):
        text, links, images = strip_html(html_content)

        analyze_text_dic = analyze_text(text)


        processed_file = {
            "source_file": f"page_{i}.html",
            "text": text,
            "statistics": {
                "word_count": analyze_text_dic["words"],
                "sentence_count": analyze_text_dic["sentences"],
                "paragraph_count": analyze_text_dic["paragraphs"],
                "avg_word_length": analyze_text_dic["avg_word_length"]
            },
            "links": links,
            "images": images,
            "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        }

        with open(f"/shared/processed/page_{i}.json", "w", encoding="utf-8") as f:
            json.dump(processed_file, f, ensure_ascii=False, indent=4)


        i += 1


    # Write completion status
    status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "HTML files": i - 1,
        "successful": sum(1 for r in results if r["status"] == "success"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "results": results
    }
    
    with open("/shared/status/process_complete.json", 'w') as f:
        json.dump(status, f, indent=2)
    
    print(f"[{datetime.now(timezone.utc).isoformat()}] Processor complete", flush=True)



if __name__ == "__main__":
    main()
