

from collections import defaultdict, Counter
import os
print(f"🧨 THIS IS scraper.py FROM: {os.path.abspath(__file__)}")
print(f"📂 Current working directory: {os.getcwd()}")

import json
import nltk
import re
from urllib.parse import urlparse, urljoin
import logging
from bs4 import BeautifulSoup
from utils import get_logger

from collections import deque
from urllib.parse import urldefrag
import builtins
print("🧨 DEBUG: My scraper.py was loaded")
builtins.__SCRAPER_LOADED__ = True
import nltk
nltk.download('punkt', force=True)

import time
logger = get_logger("Scraper", "SCRAPER")
# logger.info("🧠 Scraper logging has been initialized.")




'''SIMILARITY CHECK USING SIM HASHING'''
import hashlib
def tokenize(text):
    """Simple word tokenizer that ignores stop words and very short words"""
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    return words


def hash_token(token):
    """Hash a token into a 64-bit binary string"""
    return bin(int(hashlib.md5(token.encode('utf-8')).hexdigest(), 16))[2:].zfill(64)


def simhash(text):
    """Compute the SimHash of a text document"""
    tokens = tokenize(text)
    weights = {}
    # You can weigh by frequency or TF-IDF; this uses frequency
    for token in tokens:
        weights[token] = weights.get(token, 0) + 1
    vector = [0] * 64
    for token, weight in weights.items():
        hashbits = hash_token(token)
        for i in range(64):
            if hashbits[i] == '1':
                vector[i] += weight
            else:
                vector[i] -= weight
    # Build final fingerprint
    fingerprint = ''.join(['1' if v > 0 else '0' for v in vector])
    return fingerprint


def hamming_distance(hash1, hash2):
    """Compute Hamming distance between two binary strings"""
    return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

def are_similar(text1, text2, threshold=3):
    """Check if two texts are similar based on SimHash (lower is more similar)"""
    hash1 = simhash(text1)
    hash2 = simhash(text2)
    return hamming_distance(hash1, hash2) <= threshold





# try:
#     nltk.data.find('corpora/stopwords')
#     # logger.info("🧠 downloaded ntlk.")
# except LookupError:
#     nltk.download('stopwords', quiet=True)


try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')



subdomains = defaultdict(set) # {subdomain: set(urls)} - tracks URLs per subdomain
page_word_counts = {}        # {url: word_count} - tracks word counts for each page
unique_pages = set()         # Tracks unique URLs (defragmented)
word_counter = Counter()     # Tracks global word frequencies (for top 50)
url_fingerprints = {}        # Tracks content fingerprints to detect similar pages
logger.info("✅ Initialized globals.") #prints



def scraper(url, resp):
    print(f"💥 scraper() was called with: {url}") #does not print


    logger.info(f"[SCRAPER] Processing: {url}")

    defragmented_url, _ = urldefrag(url)

    if defragmented_url in unique_pages or resp.status != 200 or resp.raw_response is None:
        return []

    unique_pages.add(defragmented_url)

    try:
        logger.info("🧠 about to try beautiful soup.")

        soup = BeautifulSoup(resp.raw_response.content, "lxml")

        # Word and analytics processing

        #gets text
        text = soup.get_text()

        #should have tokenized the word but nltk sucks
        tokens = nltk.word_tokenize(text)

        #loads the stopwords
        stopwords = set(nltk.corpus.stopwords.words('english'))

        #adds the words into a list if they not in stopwords (for analytics later)
        words = [w.lower() for w in tokens if w.isalpha() and w.lower() not in stopwords]


        #tracks word count for each page
        page_word_counts[defragmented_url] = len(words)

        #counts words
        word_counter.update(words)

        #parsed the defragmented url
        parsed = urlparse(defragmented_url)


        #if the netlocation is in uci.edu
        if parsed.netloc.endswith("uci.edu"):

            #add it to the subdomain
            subdomains[parsed.netloc].add(defragmented_url)

        logger.info(f"✅ ABOUT TO CALL EXTRAXT NEXT LINK")
        #extracts next link
        links = extract_next_links(soup, resp.url)

        #returns a list of links that are valid
        return [link for link in links if is_valid(link)]

    except Exception as e:
        print(f"Error processing page {defragmented_url}: {e}")
        return []


def extract_next_links(url, resp):

    
    logger.info("🧠 inside extract_next_link.") 
    links = []

    #try once
    if resp.raw_response is None:
        logger.warning("❌ raw_response is None.")
    else:
        logger.info(f"✅ raw_response.content length: {len(resp.raw_response.content)}")



    try:
        
        logger.info(f"IN TRY ")

        #if theres no response, skip the url and return nothing
        if resp.status != 200 or resp.raw_response is None:
            logger.info(f"❗Skipping {url} due to status {resp.status} or missing response")
            return []


        #use beautifulsoup to parse the content 
        soup = BeautifulSoup(resp.raw_response.content, "lxml")

        #find all the a tags
        a_tags = soup.find_all('a', href=True)
        logger.info(f"🔗 Found {len(a_tags)} <a> tags in {url}")

        #looking if they have an href and convert it to a url using urljoin
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(resp.url, href)
            clean_url, _ = urldefrag(absolute_url)

            links.append(clean_url)
            logger.info(f"😏 Appended {clean_url} to links list")

    except Exception as e:
        print(f"Error processing page {url}: {e}")
    
    logger.info(f"[LINKS] Extracted {len(links)} raw links from {resp.url}")
    logger.info(f"Found {len(links)} links on {url}")


    return links

# Enforce allowed domains


def is_valid(url):
    logger.info("🧠 inside is_valid.")

    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            logger.info("⚠️ BOUT TO RETURN FALSE IN PARSED SCHEME THING.")
            return False

        # Avoid trap-like URLs
        if len(url) > 250:
            logger.info(f"REJECTED {url} -- TOO LONG")
            logger.info("⚠️ BOUT TO RETURN FALSE IN TOO LONG URL.")
            return False
        
        #sorts out un
        if re.search(r'(calendar|events|replytocom|sort|session|share|utm_|page=\d+|view=|id=|offset=)', url.lower()):
            return False
        

        if re.search(r'(\/.+\/)\1{2,}', parsed.path):
            logger.info(f"[VALIDATION] Rejected {url} - trap pattern.")
            logger.info("⚠️ BOUT TO RETURN FALSE IN RE.SEARCH THING.")
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
        
        # logger.info(f"[VALIDATION] Evaluating: {url}")



        # Skip unwanted file types
        logger.info("😬 about to return whatever is not re match")
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

# logger.info("🧠 about to make the report.")

import atexit
atexit.register(generate_report)