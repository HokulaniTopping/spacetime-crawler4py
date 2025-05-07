

from collections import defaultdict, Counter
import os
import json
import nltk
import re
from urllib.parse import urlparse, urljoin
import logging
from bs4 import BeautifulSoup
from utils import get_logger

from collections import deque
from urllib.parse import urldefrag

import time
logger = get_logger("Scraper", "SCRAPER")
logger.info("🧠 Scraper logging has been initialized.")

'''SIMILARITY CHECK'''
# Check for content similarity with previously seen pages
#         fingerprint = get_page_fingerprint(text)
#         if fingerprint in url_fingerprints.values():
#             logger.info(f"Skipping similar content page: {url}")
#             return links
#         url_fingerprints[defragmented_url] = fingerprint


# def get_page_fingerprint(text):
#     """Create a simple fingerprint for a page to detect similar content"""
#     # Get most common 15 words as a fingerprint
#     words = re.findall(r'\b[a-zA-Z]{3,15}\b', text.lower())
#     filtered_words = [w for w in words if w not in STOP_WORDS]
#     most_common = Counter(filtered_words).most_common(15)
#     return " ".join([word for word, _ in most_common])

# try:
#     nltk.data.find('corpora/stopwords')
# except LookupError:
#     nltk.download('stopwords', quiet=True)


try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    logger.info("🧠 EXCEPTING LOOKUP ERROR.")

    nltk.download('punkt', quiet=True)

logger.info("🧠 downloaded ntlk.")


subdomains = defaultdict(set) # {subdomain: set(urls)} - tracks URLs per subdomain
page_word_counts = {}        # {url: word_count} - tracks word counts for each page
unique_pages = set()         # Tracks unique URLs (defragmented)
word_counter = Counter()     # Tracks global word frequencies (for top 50)
url_fingerprints = {}        # Tracks content fingerprints to detect similar pages
logger.info("🧠 Initialized globals.")



def scraper(url, resp):
    logger.info("🧠 inside scraper.")

    logger.info(f"[SCRAPER] Processing: {url}")

    defragmented_url, _ = urldefrag(url)

    if defragmented_url in unique_pages or resp.status != 200 or resp.raw_response is None:
        return []

    unique_pages.add(defragmented_url)

    try:
        logger.info("🧠 about to try beautiful soup.")

        soup = BeautifulSoup(resp.raw_response.content, "lxml")
        
        # Word and analytics processing
        text = soup.get_text()
        tokens = nltk.word_tokenize(text)
        stopwords = set(nltk.corpus.stopwords.words('english'))
        words = [w.lower() for w in tokens if w.isalpha() and w.lower() not in stopwords]

        page_word_counts[defragmented_url] = len(words)
        word_counter.update(words)

        parsed = urlparse(defragmented_url)
        if parsed.netloc.endswith("uci.edu"):
            subdomains[parsed.netloc].add(defragmented_url)

        links = extract_next_links(soup, resp.url)
        return [link for link in links if is_valid(link)]

    except Exception as e:
        print(f"Error processing page {defragmented_url}: {e}")
        return []


def extract_next_links(url, resp):
    logger.info("🧠 inside extract_next_link.")

    logger.info(f"[LINKS] Extracted {len(links)} links from {resp.url}")
    logger.info(f"Found {len(links)} links on {url}")

    links = []

    try:
        if resp.status != 200 or resp.raw_response is None:
            return []

        soup = BeautifulSoup(resp.raw_response.content, "lxml")

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(resp.url, href)
            clean_url, _ = urldefrag(absolute_url)
            links.append(clean_url)

    except Exception as e:
        print(f"Error processing page {url}: {e}")
    
    logger.info(f"[LINKS] Extracted {len(links)} raw links from {resp.url}")

    return links

# Enforce allowed domains


def is_valid(url):
    logger.info("🧠 inside is_valid.")

    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        # Avoid trap-like URLs
        if len(url) > 250:
            logger.info(f"REJECTED {url} -- TOO LONG")
            return False
        
        # if re.search(r'(calendar|events|replytocom|sort|session|share|utm_|page=\d+|view=|id=|offset=)', url.lower()):
        #     return False
        if re.search(r'(\/.+\/)\1{2,}', parsed.path):
            logger.info(f"[VALIDATION] Rejected {url} - trap pattern.")
            return False
        

        domain = parsed.netloc.lower()

        # if not any(domain in parsed.netloc for domain in allowed_domains):
        #     return False
        allowed_domains = {
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
            "today.uci.edu"
        }
        if not any(domain.endswith(seed) for seed in allowed_domains):
            logger.info(f"Rejected {url} due to unmatched domain.")
            return False

        # if not any(domain in parsed.netloc for domain in allowed_domains):
        #     logger.info(f"Rejected {url} due to unmatched domain.")
        #     return False
        
        logger.info(f"[VALIDATION] Evaluating: {url}")



        # Skip unwanted file types
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for", url)
        return False



def generate_report():
    """Generate the final report based on crawled data"""
    report = []
    
    # Create report directory if it doesn't exist
    if not os.path.exists("report"):
        os.makedirs("report")
    
    # 1. Number of unique pages
    report.append(f"1. Number of unique pages: {len(unique_pages)}")
    
    # 2. Longest page
    if page_word_counts:
        longest_url = max(page_word_counts, key=page_word_counts.get)
        report.append(f"\n2. Longest page: {longest_url} with {page_word_counts[longest_url]} words")
    
    # 3. 50 most common words
    report.append("\n3. 50 most common words:")
    for word, count in word_counter.most_common(50):
        report.append(f"   {word}: {count}")
    
    # 4. Subdomains
    report.append("\n4. Subdomains in uci.edu:")
    sorted_subdomains = sorted(subdomains.items())
    for subdomain, urls in sorted_subdomains:
        report.append(f"   {subdomain}: {len(urls)}")
    
    # Write report to file
    with open("report/crawler_report.txt", "w") as f:
        f.write("\n".join(report))
    
    # Also save raw data for further analysis if needed
    with open("report/page_word_counts.json", "w") as f:
        # Convert set values to list for JSON serialization
        json_friendly_subdomains = {k: list(v) for k, v in subdomains.items()}
        json.dump({
            "unique_page_count": len(unique_pages),
            "page_word_counts": page_word_counts,
            "common_words": dict(word_counter.most_common(200)),
            "subdomains": json_friendly_subdomains
        }, f, indent=2)
    
    return "\n".join(report)

logger.info("🧠 about to make the report.")

import atexit
atexit.register(generate_report)

