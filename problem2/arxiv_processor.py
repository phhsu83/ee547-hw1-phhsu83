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

STOPWORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
             'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
             'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
             'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
             'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
             'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
             'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'}

def to_iso8601_z(dt_str):
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") 


def main():

    # Check command line arguments is correct 
    if len(sys.argv) != 4:
        print("python arxiv_processor.py <search_query> <max_results> <output_dir>")
        sys.exit(1)

    search_query = sys.argv[1]
    try:
        max_results = int(sys.argv[2])
        if not (1 <= max_results <= 100):
            raise ValueError
    except ValueError:
        print("integer between 1 and 100")
        sys.exit(1)
    output_dir = sys.argv[3]

    # Check the output dirctory exists
    os.makedirs(output_dir, exist_ok=True)


    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} Starting ArXiv query: {search_query}\n")

    # Query the ArXiv API and fetch up to the results
    base_url = "http://export.arxiv.org/api/query?"

    params = {
        "search_query": search_query, 
        "start": 0, 
        "max_results": max_results 
    }

    query_url = base_url + urllib.parse.urlencode(params)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(query_url) as response:
                data = response.read() # return XML (bytes)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                logging.warning("HTTP 429 Rate limit reached. Waiting 3 seconds before retry...")
                time.sleep(3)
            else:
                now_utc = datetime.now(timezone.utc)
                timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
                with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
                    log_file.write(f"{timestamp} HTTPError: {e}\n")
                sys.exit(1)
        except Exception as e:
            now_utc = datetime.now(timezone.utc)
            timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
            with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
                log_file.write(f"{timestamp} Network error: {e}\n")
            sys.exit(1)

    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} Fetched {max_results} results from ArXiv API\n")


    # Parse the XML
    namespace = {"atom": "http://www.w3.org/2005/Atom"}
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        now_utc = datetime.now(timezone.utc)
        timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
        with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp} Invalid XML: {e}\n")
        
        root = ET.Element("root") # Create an empty root node to avoid loop errors

    papers = []

    total_abstracts = 0
    total_words_global = 0
    unique_words_global = set()
    avg_abstract_length = 0.0
    longest_abstract_words = 0
    shortest_abstract_words = 1000

    top_50_words = Counter()

    uppercase_terms = []
    numeric_terms = []
    hyphenated_terms = []

    category_distribution = {}

    start_time = time.perf_counter()
    for entry in root.findall("atom:entry", namespace):

        try:
            arxiv_id = entry.find("atom:id", namespace)
            title = entry.find("atom:title", namespace)
            summary = entry.find("atom:summary", namespace)
            published = entry.find("atom:published", namespace)
            updated = entry.find("atom:updated", namespace)
            authors = entry.findall("atom:author", namespace)

            if not (arxiv_id is not None and title is not None and summary is not None 
                    and published is not None and updated is not None and authors):
                now_utc = datetime.now(timezone.utc)
                timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
                with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
                    log_file.write(f"{timestamp} Skipping paper due to missing fields\n")
                continue  
            if not authors:
                now_utc = datetime.now(timezone.utc)
                timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
                with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
                    log_file.write(f"{timestamp} Skipping paper {arxiv_id} due to missing authors\n")
                continue
        except Exception as e:
            now_utc = datetime.now(timezone.utc)
            timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
            with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
                log_file.write(f"{timestamp} Skipping paper due to processing error: {e}\n")
            continue

        arxiv_id = entry.find("atom:id", namespace).text
        title = entry.find("atom:title", namespace).text.strip()
        authors = [author.find("atom:name", namespace).text for author in entry.findall("atom:author", namespace)]
        abstract = entry.find("atom:summary", namespace).text.strip()
        categories = [c.attrib["term"] for c in entry.findall("atom:category", namespace)]
        published = to_iso8601_z(entry.find("atom:published", namespace).text)
        updated = to_iso8601_z(entry.find("atom:updated", namespace).text)
        total_abstracts += 1
        for cat in categories:
            category_distribution[cat] = category_distribution.get(cat, 0) + 1

        now_utc = datetime.now(timezone.utc)
        timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
        with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp} Processing paper: {arxiv_id}\n")

        # Part B
        words = re.findall(r"\w+(?:[-']\w+)*", abstract, flags=re.UNICODE) # List[str]
        total_words = len(words)
        total_words_global += total_words
        unique_words = len(set(words))
        unique_words_global.update(set(words))
        # longest_words = max(words, key=len)
        # longest_abstract_words = max(longest_abstract_words, len(longest_words))
        # shortest_words = min(words, key=len)
        # shortest_abstract_words = min(shortest_abstract_words, len(shortest_words))
        longest_abstract_words = max(longest_abstract_words, total_words)
        shortest_abstract_words = min(shortest_abstract_words, total_words)

        lower_words = [w.lower() for w in words]
        freq_lower_words_dic = Counter(lower_words)
        freq_lower_words_dic = {w: c for w, c in freq_lower_words_dic.items() if w not in STOPWORDS}
        # top_20_freq_lower_words_dic = freq_lower_words_dic.most_common(20) # return [(apple, 10), (banana, 7), ...] # wrong
        top_20_freq_lower_words_dic = sorted(freq_lower_words_dic.items(), key=lambda x: x[1], reverse=True)[:20]
        top_20_freq_lower_words = [pair[0] for pair in top_20_freq_lower_words_dic]
        top_50_words += freq_lower_words_dic

        avg_word_length = sum(len(w) for w in words) / total_words if total_words > 0 else 0

        sentences = re.split(r"[.!?]", abstract)
        sentences = [s for s in sentences if s.strip()]
        total_sentences = len(sentences)
        avg_words_per_sentence = total_words / total_sentences if total_sentences > 0 else 0
        
        if not sentences:
            longest_sent = ""
            shortest_sent = ""
        else:
            longest_sent = sentences[0]
            shortest_sent = sentences[0]

        for sent in sentences:
            words_count = len(re.findall(r"\w+(?:[-']\w+)*", sent, flags=re.UNICODE))
            curr_longest_count = len(re.findall(r"\w+(?:[-']\w+)*", longest_sent, flags=re.UNICODE))
            curr_shortest_count = len(re.findall(r"\w+(?:[-']\w+)*", shortest_sent, flags=re.UNICODE))

            if words_count > curr_longest_count:
                # current sentence is longer 
                longest_sent = sent
            if words_count < curr_shortest_count:
                # current sentence is shorter 
                shortest_sent = sent

        # all words containing uppercase
        words_with_upper = re.findall(r"\b\w*[A-Z]\w*\b", abstract)
        uppercase_terms.extend(words_with_upper)
        # all words containing numbers
        words_with_num = re.findall(r"\b\w*[0-9]\w*\b", abstract)
        numeric_terms.extend(words_with_num)
        # all hyphenated terms
        words_with_hyphen = re.findall(r"\b\w+-\w+\b", abstract)
        hyphenated_terms.extend(words_with_hyphen)


        abstract_stats = {
            "total_words": total_words,
            "unique_words": unique_words,
            "total_sentences": total_sentences,
            "avg_words_per_sentence": avg_words_per_sentence,
            "avg_word_length": avg_word_length
        }
        paper = {
            "arxiv_id": arxiv_id,
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "categories": categories,
            "published": published,
            "updated": updated,
            "abstract_stats": abstract_stats,
        }
        
        papers.append(paper)


    papers_path = os.path.join(output_dir, "papers.json")
    with open(papers_path, "w", encoding="utf-8") as f:
        json.dump(papers, f, ensure_ascii=False, indent=4)


    corpus_analysis = {
        "query": search_query,
        "papers_processed": total_abstracts,
        "processing_timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "corpus_stats": {
            "total_abstracts": total_abstracts,
            "total_words": total_words_global,
            "unique_words_global": len(unique_words_global),
            "avg_abstract_length": total_words_global / total_abstracts,
            "longest_abstract_words": longest_abstract_words,
            "shortest_abstract_words": shortest_abstract_words
        },
        "top_50_words": [{"word": w, "frequency": f} for w, f in top_50_words.most_common(50)],
        "technical_terms": {
            "uppercase_terms": uppercase_terms,
            "numeric_terms": numeric_terms,
            "hyphenated_terms": hyphenated_terms
        },
        "category_distribution": category_distribution
    }

    corpus_analysis_path = os.path.join(output_dir, "corpus_analysis.json")
    with open(corpus_analysis_path, "w", encoding="utf-8") as f:
        json.dump(corpus_analysis, f, ensure_ascii=False, indent=4)


    end_time = time.perf_counter()
    response_time = (end_time - start_time)
    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    with open(os.path.join(output_dir, "processing.log"), "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} Completed processing: {total_abstracts} papers in {response_time} seconds\n")


    
if __name__ == "__main__":
    main()
