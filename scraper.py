
# import re
# from urllib.parse import urlparse, urljoin
# from bs4 import BeautifulSoup
# from collections import defaultdict, Counter
# import requests
# import time
# import nltk
# from nltk.corpus import stopwords
# from collections import deque
# import os
# import json
# from urllib.robotparser import RobotFileParser
# import random
# import logging

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("crawler.log"),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)

# # Try to load stopwords, download if necessary
# try:
#     nltk.data.find('corpora/stopwords')
# except LookupError:
#     nltk.download('stopwords', quiet=True)

# # Global analytics trackers
# page_word_counts = {}        # {url: word_count} - tracks word counts for each page
# word_counter = Counter()     # Tracks global word frequencies (for top 50)
# subdomains = defaultdict(set) # {subdomain: set(urls)} - tracks URLs per subdomain
# unique_pages = set()         # Tracks unique URLs (defragmented)
# url_fingerprints = {}        # Tracks content fingerprints to detect similar pages


# MAX_LINKS_PER_PAGE = 100     # Trap detection - max links to extract from a page 
# MAX_URL_LENGTH = 150         # Avoid extremely long URLs (likely traps)
# MIN_WORD_COUNT = 20          # Skip pages with too little text content
# DEFAULT_CRAWL_DELAY = 1.0    # Default 1 second between requests to same domain
# MIN_CRAWL_DELAY = 0.5        # Minimum crawl delay (for domains with shorter specified delay)
# MAX_PAGES_TO_CRAWL = 10000   # Maximum number of pages to crawl
# STOP_WORDS = set(stopwords.words('english'))
# USER_AGENT = "UCIWebCrawler/1.0"  # Identify crawler in requests
# RESPECT_ROBOTS_TXT = True    # Toggle robots.txt compliance
# RETRY_COUNT = 2              # Number of retries for failed requests
# RETRY_BACKOFF = 2.0          # Exponential backoff factor for retries
# # Cache for robots.txt files
# robots_cache = {}

# def get_robots_parser(domain):
#     """
#     Get or create a RobotFileParser for the specified domain.
#     """
#     if domain in robots_cache:
#         return robots_cache[domain]
        
#     # crating a new parser
#     parser = RobotFileParser()
    
#     try:
#         robots_url = f"https://{domain}/robots.txt"
#         parser.set_url(robots_url)
        
#         # Fetch with a timeout
#         response = requests.get(robots_url, timeout=5, headers={"User-Agent": USER_AGENT})
        
#         if response.status_code == 200:
#             parser.parse(response.text.splitlines())
#             logger.info(f"Successfully parsed robots.txt for {domain}")
#         else:
#             logger.warning(f"Could not fetch robots.txt for {domain}, status code: {response.status_code}")
#             # If we can't fetch the file, assume no restrictions
#             parser.allow_all = True
#     except Exception as e:
#         logger.warning(f"Error fetching robots.txt for {domain}: {e}")
#         # If there's an error, assume no restrictions
#         parser.allow_all = True
        
#     # Cache the parser
#     robots_cache[domain] = parser
#     return parser

# def can_fetch(url):
#     """
#     Check if the URL can be fetched according to robots.txt rules.
#     """
#     if not RESPECT_ROBOTS_TXT:
#         return True
        
#     parsed_url = urlparse(url)
#     domain = parsed_url.netloc
    
#     if not domain:
#         return False
        
#     parser = get_robots_parser(domain)
#     return parser.can_fetch(USER_AGENT, url)

# def get_crawl_delay(domain):
#     """
#     Get the crawl delay for a domain from its robots.txt.
#     Returns the default delay if not specified.
#     """
#     if not RESPECT_ROBOTS_TXT:
#         return DEFAULT_CRAWL_DELAY
        
#     parser = get_robots_parser(domain)
    
#     try:
#         # Get crawl delay for our user agent
#         delay = parser.crawl_delay(USER_AGENT)
        
#         # If specified, use at least the minimum delay
#         if delay is not None:
#             return max(float(delay), MIN_CRAWL_DELAY)
#     except Exception as e:
#         logger.warning(f"Error getting crawl delay for {domain}: {e}")
        
#     # Default to our configured delay
#     return DEFAULT_CRAWL_DELAY

# def extract_next_links(url, resp):
#     """
#     Extract links from the response and process page content for analytics.
#     """
#     links = []
    
#     # Skip if response is invalid
#     if resp.status_code != 200 or not resp.content:
#         return links
    
#     # Process URL for analytics
#     defragmented_url = url.split('#')[0]
#     unique_pages.add(defragmented_url)
    
#     try:
#         # Parse content with BeautifulSoup
#         soup = BeautifulSoup(resp.content, 'html.parser')
        
#         # Process page content for analytics
#         process_page_content(defragmented_url, soup)
        
#         # Check if page has enough content to be worth crawling
#         text = soup.get_text()
#         if len(text.strip().split()) < MIN_WORD_COUNT:
#             logger.info(f"Skipping low-information page: {url}")
#             return links
        
#         # Check for content similarity with previously seen pages
#         fingerprint = get_page_fingerprint(text)
#         if fingerprint in url_fingerprints.values():
#             logger.info(f"Skipping similar content page: {url}")
#             return links
#         url_fingerprints[defragmented_url] = fingerprint
        
#         # Extract links
#         link_counter = 0
#         for a_tag in soup.find_all('a', href=True):
#             # Skip if we've extracted too many links (potential trap)
#             if link_counter > MAX_LINKS_PER_PAGE:
#                 logger.warning(f"Too many links on page {url} - potential trap")
#                 break
                
#             href = a_tag['href']
#             # Convert to absolute URL
#             absolute_url = urljoin(url, href)
#             # Remove fragment
#             defragmented_url = absolute_url.split('#')[0]
            
#             # Check if URL is allowed by robots.txt
#             if can_fetch(defragmented_url):
#                 links.append(defragmented_url)
#                 link_counter += 1
#             else:
#                 logger.info(f"Skipping {defragmented_url} - disallowed by robots.txt")
        
#         return links
        
#     except Exception as e:
#         logger.error(f"Error processing {url}: {e}")
#         return links

# def process_page_content(url, soup):
#     """Process page content for analytics reports"""
#     # Remove script and style elements
#     for element in soup(["script", "style"]):
#         element.extract()
    
#     # Get text content
#     text = soup.get_text(separator=' ')
    
#     # Count words (excluding stop words)
#     words = re.findall(r'\b[a-zA-Z]{3,15}\b', text.lower())
#     filtered_words = [word for word in words if word not in STOP_WORDS]
    
#     # Update analytics
#     page_word_counts[url] = len(filtered_words)
#     word_counter.update(filtered_words)
    
#     # Extract subdomain
#     parsed_url = urlparse(url)
#     if "uci.edu" in parsed_url.netloc:
#         subdomain = parsed_url.netloc.split('.')[0] if len(parsed_url.netloc.split('.')) > 2 else parsed_url.netloc
#         subdomains[subdomain].add(url)

# def get_page_fingerprint(text):
#     """Create a simple fingerprint for a page to detect similar content"""
#     # Get most common 15 words as a fingerprint
#     words = re.findall(r'\b[a-zA-Z]{3,15}\b', text.lower())
#     filtered_words = [w for w in words if w not in STOP_WORDS]
#     most_common = Counter(filtered_words).most_common(15)
#     return " ".join([word for word, _ in most_common])

# def is_valid(url):
#     """
#     Check if the URL is valid for crawling according to the assignment requirements.
#     """
#     try:
#         parsed = urlparse(url)
        
#         # Check for valid schemes
#         if parsed.scheme not in {'http', 'https'}:
#             return False
            
#         # Check URL length to avoid traps
#         if len(url) > MAX_URL_LENGTH:
#             return False
            
#         # Skip non-HTML resources
#         if re.match(
#             r".*\.(css|js|bmp|gif|jpe?g|ico"
#             + r"|png|tiff?|mid|mp2|mp3|mp4"
#             + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
#             + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
#             + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
#             + r"|epub|dll|cnf|tgz|sha1"
#             + r"|thmx|mso|arff|rtf|jar|csv"
#             + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
#             return False
            
#         # Detect potential calendar traps
#         if re.search(r'/(calendar|events?)/.*/(day|week|month|year)', parsed.path.lower()):
#             return False
            
#         # Check for repetitive patterns in URL (potential trap)
#         path_segments = parsed.path.split('/')
#         if len(path_segments) > 8:
#             return False
            
#         # Check for repeated path segments (potential trap)
#         if len(path_segments) != len(set(path_segments)) and len(path_segments) > 3:
#             # Count segment frequencies
#             segment_counts = Counter(path_segments)
#             # If any segment appears more than 2 times, likely a trap
#             if any(count > 2 for segment, count in segment_counts.items() if segment):
#                 return False
        
#         # Check for dynamic URL patterns
#         dynamic_patterns = [
#             r'print=', r'printable=', r'print_view=',
#             r'replytocom=', r'/comment-page-', r'/trackback/',
#             r'/feed/', r'/atom/', r'/rss/', r'/cgi-bin/'
#         ]
#         if any(re.search(pattern, url) for pattern in dynamic_patterns):
#             return False
            
#         # Check for allowed domains according to assignment
#         allowed_domains = [
#             r".*\.ics\.uci\.edu.*",
#             r".*\.cs\.uci\.edu.*",
#             r".*\.informatics\.uci\.edu.*",
#             r".*\.stat\.uci\.edu.*",
#             r"today\.uci\.edu/department/information_computer_sciences/.*"
#         ]
        
#         # Check if URL matches any allowed domain pattern
#         domain_matched = False
#         for pattern in allowed_domains:
#             if re.match(pattern, parsed.netloc) or (
#                 parsed.netloc == "today.uci.edu" and 
#                 parsed.path.startswith("/department/information_computer_sciences/")
#             ):
#                 domain_matched = True
#                 break
                
#         if not domain_matched:
#             return False
            
#         # Check if URL is allowed by robots.txt
#         if not can_fetch(url):
#             return False
            
#         return True
        
#     except Exception as e:
#         logger.error(f"Error validating URL {url}: {e}")
#         return False

# def scraper(url, resp):
#     """
#     The main scraper function required by the crawler framework.
#     """
#     links = extract_next_links(url, resp)
#     return [link for link in links if is_valid(link)]

# def generate_report():
#     """Generate the final report based on crawled data"""
#     report = []
    
#     # Create report directory if it doesn't exist
#     if not os.path.exists("report"):
#         os.makedirs("report")
    
#     # 1. Number of unique pages
#     report.append(f"1. Number of unique pages: {len(unique_pages)}")
    
#     # 2. Longest page
#     if page_word_counts:
#         longest_url = max(page_word_counts, key=page_word_counts.get)
#         report.append(f"\n2. Longest page: {longest_url} with {page_word_counts[longest_url]} words")
    
#     # 3. 50 most common words
#     report.append("\n3. 50 most common words:")
#     for word, count in word_counter.most_common(50):
#         report.append(f"   {word}: {count}")
    
#     # 4. Subdomains
#     report.append("\n4. Subdomains in uci.edu:")
#     sorted_subdomains = sorted(subdomains.items())
#     for subdomain, urls in sorted_subdomains:
#         report.append(f"   {subdomain}: {len(urls)}")
    
#     # Write report to file
#     with open("report/crawler_report.txt", "w") as f:
#         f.write("\n".join(report))
    
#     # Also save raw data for further analysis if needed
#     with open("report/page_word_counts.json", "w") as f:
#         # Convert set values to list for JSON serialization
#         json_friendly_subdomains = {k: list(v) for k, v in subdomains.items()}
#         json.dump({
#             "unique_page_count": len(unique_pages),
#             "page_word_counts": page_word_counts,
#             "common_words": dict(word_counter.most_common(200)),
#             "subdomains": json_friendly_subdomains
#         }, f, indent=2)
    
#     logger.info("Report saved to report/crawler_report.txt")
#     logger.info("Raw data saved to report/page_word_counts.json")
    
#     return "\n".join(report)

# def process_url_with_retry(url, domain_last_access, crawled, queue):
#     """Process a URL with retry logic for failed requests."""
#     parsed = urlparse(url)
#     domain = parsed.netloc
    
#     # Get crawl delay from robots.txt or use default
#     crawl_delay = get_crawl_delay(domain)
    
#     # Check politeness delay
#     current_time = time.time()
#     if domain in domain_last_access:
#         time_since_last_access = current_time - domain_last_access[domain]
#         if time_since_last_access < crawl_delay:
#             # Wait to be polite
#             sleep_time = crawl_delay - time_since_last_access
#             time.sleep(sleep_time)
    
#     # Update last access time
#     domain_last_access[domain] = time.time()
    
#     # Track retry attempts
#     retry_count = 0
#     success = False
    
#     while retry_count <= RETRY_COUNT and not success:
#         try:
#             # Add some jitter to avoid synchronized requests
#             jitter = random.uniform(0, 0.5)
#             if retry_count > 0:
#                 backoff_time = RETRY_BACKOFF ** retry_count + jitter
#                 logger.info(f"Retry {retry_count} for {url}, waiting {backoff_time:.2f}s")
#                 time.sleep(backoff_time)
            
#             # Fetch page with proper headers
#             headers = {
#                 "User-Agent": USER_AGENT,
#                 "Accept": "text/html,application/xhtml+xml,application/xml",
#                 "Accept-Language": "en-US,en;q=0.9",
#                 "Connection": "keep-alive"
#             }
            
#             resp = requests.get(url, timeout=10, headers=headers)
            
#             # Handle specific response codes
#             if resp.status_code == 429:  # Too Many Requests
#                 retry_count += 1
#                 # Use Retry-After header if available, or backoff
#                 retry_after = int(resp.headers.get('Retry-After', 60))
#                 logger.warning(f"Rate limited on {url}, waiting {retry_after}s")
#                 time.sleep(retry_after)
#                 continue
#             elif resp.status_code == 200:
#                 success = True
#             else:
#                 logger.warning(f"Got status code {resp.status_code} for {url}")
#                 if 500 <= resp.status_code < 600:  # Server errors
#                     retry_count += 1
#                     continue
#                 else:
#                     # Don't retry for other status codes
#                     break
            
#             # Create a response object similar to what spacetime framework would provide
#             class ResponseWrapper:
#                 def __init__(self, response):
#                     self.status_code = response.status_code
#                     self.content = response.content
#                     self.url = response.url
                    
#             resp_wrapper = ResponseWrapper(resp)
            
#             # Process the page
#             next_links = scraper(url, resp_wrapper)
            
#             # Mark URL as crawled
#             crawled.add(url)
            
#             # Add new links to queue
#             for link in next_links:
#                 if link not in crawled:
#                     queue.append(link)
                    
#             return True
                
#         except Exception as e:
#             logger.error(f"Error crawling {url}: {e}")
#             retry_count += 1
    
#     return success

# def standalone_crawler():
#     """
#     Standalone crawler for testing - uses the requests library directly
#     instead of the spacetime framework.
#     """
#     # Seed URLs from the assignment
#     seed_urls = [
#         'https://www.ics.uci.edu/',
#         'https://www.cs.uci.edu/',
#         'https://www.informatics.uci.edu/',
#         'https://www.stat.uci.edu/',
#         'https://today.uci.edu/department/information_computer_sciences/'
#     ]
    
#     logger.info(f"Starting standalone crawler with {len(seed_urls)} seed URLs")
    
#     # Track last access time per domain for politeness
#     domain_last_access = {}
    
#     # Queue of URLs to process
#     queue = deque(seed_urls)
    
#     # Track crawled URLs
#     crawled = set()
    
#     # Statistics
#     stats = {
#         "success": 0,
#         "failed": 0,
#         "skipped_robots": 0,
#         "start_time": time.time()
#     }

#     # while queue and len(crawled) < MAX_PAGES_TO_CRAWL:
#     while queue:
#         url = queue.popleft()
        
#         # Skip if already crawled
#         if url in crawled:
#             continue
        
#         # Check if URL is allowed by robots.txt
#         if not can_fetch(url):
#             logger.info(f"Skipping {url} - disallowed by robots.txt")
#             stats["skipped_robots"] += 1
#             continue
            
#         logger.info(f"Crawling: {url} [{len(crawled) + 1}/{MAX_PAGES_TO_CRAWL}]")
        
#         # Process URL with retry logic
#         success = process_url_with_retry(url, domain_last_access, crawled, queue)
        
#         if success:
#             stats["success"] += 1
#         else:
#             stats["failed"] += 1
            
#         # Report progress
#         if len(crawled) % 50 == 0:
#             elapsed = time.time() - stats["start_time"]
#             pages_per_second = len(crawled) / max(1, elapsed)
#             logger.info(f"Progress: {len(crawled)} pages crawled, {len(queue)} in queue")
#             logger.info(f"Rate: {pages_per_second:.2f} pages/second, Success rate: {stats['success']/(stats['success']+stats['failed'])*100:.1f}%")
            
#     logger.info(f"Crawling complete. Processed {len(crawled)} pages.")
#     logger.info(f"Success: {stats['success']}, Failed: {stats['failed']}, Skipped (robots.txt): {stats['skipped_robots']}")
    
#     report = generate_report()
#     logger.info(report)

# if __name__ == "__main__":
#     logger.info("Starting standalone crawler with robots.txt support...")
#     standalone_crawler()



from collections import defaultdict, Counter
import os
import json
import nltk
import re
from urllib.parse import urlparse, urljoin
import logging
from bs4 import BeautifulSoup
logger = logging.getLogger(__name__)
from collections import deque
from urllib.parse import urldefrag




try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)


subdomains = defaultdict(set) # {subdomain: set(urls)} - tracks URLs per subdomain
page_word_counts = {}        # {url: word_count} - tracks word counts for each page
unique_pages = set()         # Tracks unique URLs (defragmented)
word_counter = Counter()     # Tracks global word frequencies (for top 50)
MAX_PAGES_TO_CRAWL = 100


# def scraper(url, resp):
#     links = extract_next_links(url, resp)
#     return [link for link in links if is_valid(link)]

def scraper(url, resp):
    defragmented_url, _ = urldefrag(url)

    if defragmented_url in unique_pages or resp.status != 200 or resp.raw_response is None:
        return []

    if len(unique_pages) >= MAX_PAGES_TO_CRAWL:
        return []

    unique_pages.add(defragmented_url)

    try:
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

        # Extract links using soup (no need to re-parse)
        links = extract_next_links(soup, resp.url)
        return [link for link in links if is_valid(link)]

    except Exception as e:
        print(f"Error processing page {defragmented_url}: {e}")
        return []


def extract_next_links(url, resp):
    links = []

    try:
        if resp.status != 200 or resp.raw_response is None:
            return []

        defragmented_url, _ = urldefrag(url)

        if defragmented_url in unique_pages:
            return []

        if len(unique_pages) >= MAX_PAGES_TO_CRAWL:
            return []

        unique_pages.add(defragmented_url)

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

        # Extract links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(resp.url, href)
            clean_url, _ = urldefrag(absolute_url)
            links.append(clean_url)

    except Exception as e:
        print(f"Error processing page {url}: {e}")

    return links


def is_valid(url):
    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        # Avoid trap-like URLs
        if len(url) > 250:
            return False
        if re.search(r'(calendar|events|replytocom|sort|session|share|utm_|page=\d+|view=|id=|offset=)', url.lower()):
            return False
        if re.search(r'(\/.+\/)\1{2,}', parsed.path):
            return False
        

        # Enforce allowed domains
        allowed_domains = {
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
            "today.uci.edu"
        }
        if not any(domain in parsed.netloc for domain in allowed_domains):
            return False

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






if __name__ == "__main__":
    pass