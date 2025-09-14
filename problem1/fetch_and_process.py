import sys
import os
import json
import urllib.request
import urllib.error
import re
import time
from datetime import datetime, timezone
from collections import defaultdict

def count_words(response_body: str, content_type: str):

    ct = (content_type or "").lower()
    if "text" not in ct and "json" not in ct:
        return None 

    words = re.findall(r"[A-Za-z0-9]+", response_body)

    return len(words)


def main():

    # Check command line arguments is correct 
    if len(sys.argv) != 3:
        print("python fetch_and_save.py <input_file> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    # Check the output dirctory exists
    os.makedirs(output_dir, exist_ok=True)


    # Read the input file
    with open(input_file, "r") as f:
        urls = [line.strip() for line in f if line.strip()]


    results = []

    successful_requests = 0
    total_response_time_ms = 0
    total_bytes = 0
    status_code_distribution = defaultdict(int)

    processing_start = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    for url in urls:
        try:
            start_time = time.perf_counter()

            with urllib.request.urlopen(url, timeout = 10) as response:

                status_code = response.getcode()          
               
                body = response.read() # bytes
                body_size = len(body)

                end_time = time.perf_counter()
                response_time_ms = (end_time - start_time) * 1000

                text_body = body.decode("utf-8", errors="ignore") # str
                content_type = response.headers.get("Content-Type")
                words_number = count_words(text_body, content_type)

                now_utc = datetime.now(timezone.utc)
                timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")


                response_data = {
                    "url": url,
                    "status_code": status_code,
                    "response_time_ms": response_time_ms,
                    "content_length": body_size,
                    "word_count": words_number,
                    "timestamp": timestamp,
                    "error": None
                }
                results.append(response_data)

                successful_requests += 1
                total_response_time_ms += response_time_ms
                total_bytes += body_size
                status_code_distribution[str(status_code)] += 1


        except urllib.error.HTTPError as e:
            now_utc = datetime.now(timezone.utc)
            timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
            response_data = {
                    "url": url,
                    "status_code": e.code,
                    "timestamp": timestamp,
                    "error": f"HTTPError: {e.reason}"
            }
            results.append(response_data)

            status_code_distribution[str(e.code)] += 1

            with open(os.path.join(output_dir, "errors.log"), "a", encoding="utf-8") as log_file:
                log_file.write(f"[{timestamp}] {url}: {response_data['error']}\n")

        except urllib.error.URLError as e:
            now_utc = datetime.now(timezone.utc)
            timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
            response_data = {
                    "url": url,
                    "timestamp": timestamp,
                    "error": f"URLError: {e.reason}"
            }
            results.append(response_data)

            with open(os.path.join(output_dir, "errors.log"), "a", encoding="utf-8") as log_file:
                log_file.write(f"[{timestamp}] {url}: {response_data['error']}\n")

        except Exception as e:
            now_utc = datetime.now(timezone.utc)
            timestamp = now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
            response_data = {
                    "url": url,
                    "timestamp": timestamp,
                    "error": f"OtherError: {str(e)}"
            }
            results.append(response_data)

            with open(os.path.join(output_dir, "errors.log"), "a", encoding="utf-8") as log_file:
                log_file.write(f"[{timestamp}] {url}: {response_data['error']}\n")


    responses_path = os.path.join(output_dir, "output.json")
    with open(responses_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)


    processing_end = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


    average_response_time_ms = (total_response_time_ms / successful_requests) if successful_requests else 0
    summary = {
        "total_urls": len(urls),
        "successful_requests": successful_requests,
        "failed_requests": len(urls) - successful_requests,
        "average_response_time_ms": average_response_time_ms,
        "total_bytes_downloaded": total_bytes,
        "status_code_distribution": dict(status_code_distribution),
        "processing_start": processing_start,
        "processing_end": processing_end
    }

    summary_path = os.path.join(output_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()
