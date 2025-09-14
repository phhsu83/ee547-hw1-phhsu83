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



def jaccard_similarity(doc1_words, doc2_words):
    """Calculate Jaccard similarity between two documents."""
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def wait_for_complete_file(path, timeout=300, interval=3):

    start = time.time()
    
    while time.time() - start < timeout:
        if os.path.exists(path):
            print(f"Find the completion file: {path}")
            return True

        time.sleep(interval)
    
    raise TimeoutError(f"Timeout")


def main():

    wait_for_complete_file("/shared/status/process_complete.json")
    
    all_processed_files = []
    processed_dir = "/shared/processed"
    analysis_dir = "/shared/analysis"
    os.makedirs(analysis_dir, exist_ok=True)

    for file in glob.glob(os.path.join(processed_dir, "*.json")):
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_processed_files.append(data)

    # Word frequency distribution (top 100 words)
    all_texts = " ".join(file["text"] for file in all_processed_files) # str
    words = re.findall(r"\w+(?:[-']\w+)*", all_texts, flags=re.UNICODE) # List[str]
    total_words_count = len(words)
    unique_words_count = len(set(words))

    lower_words = [w.lower() for w in words]
    words_freq_dic = Counter(lower_words)
    
    top_100_words = []
    for word, count in words_freq_dic.most_common(100):
        dic = {
            "word": word, 
            "count": count, 
            "frequency": count / total_words_count
        }
        top_100_words.append(dic)



    # Document similarity matrix (Jaccard similarity)
    document_similarity = []
    n = len(all_processed_files)
    
    for i in range(n):
        for j in range(i + 1, n):
            if i == j:
                continue
            else:
                similarity = jaccard_similarity(all_processed_files[i]["text"], all_processed_files[j]["text"])
                dic = {
                    "doc1": f"page_{i}.json", 
                    "doc2": f"page_{j}.json", 
                    "similarity": similarity
                }
                document_similarity.append(dic)


    # N-gram extraction (bigrams and trigrams)
    bigrams = list(zip(words, words[1:]))
    bigrams_freq_dic = Counter(bigrams)
    top_bigrams = []
    for word, count in bigrams_freq_dic.most_common(100):
        dic = {
            "word": word, 
            "count": count
        }
        top_bigrams.append(dic)


    # Readability metrics
    avg_word_length = sum(len(w) for w in words) / total_words_count if total_words_count > 0 else 0

    sentences = re.split(r"[.!?]", all_texts)
    sentences = [s for s in sentences if s.strip()]
    total_sentences = len(sentences)
    avg_words_per_sentence = total_words_count / total_sentences if total_sentences > 0 else 0

    complexity_score = avg_word_length * avg_words_per_sentence


    # 
    final_report = {
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "documents_processed": len(all_processed_files),
        "total_words": total_words_count,
        "unique_words": unique_words_count,
        "top_100_words": top_100_words,
        "document_similarity": document_similarity,
        "top_bigrams": top_bigrams,
        "readability": {
            "avg_sentence_length": avg_word_length,
            "avg_word_length": avg_words_per_sentence,
            "complexity_score": complexity_score
        }
    }

    with open("/shared/analysis/final_report.json", "w", encoding="utf-8") as f:
        json.dump(final_report, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()
