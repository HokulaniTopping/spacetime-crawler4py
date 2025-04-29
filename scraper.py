import re
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import defaultdict, Counter
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
import time
import requests
from collections import deque


page_word_counts = {}               # {url: word_count} for longest page
word_counter = Counter()            # Tracks word frequencies (for top 50)
subdomains = defaultdict(set)       # {subdomain: count} for uci.edu
MAX_LINKS_PER_PAGE = 100
stop_words = set(stopwords.words('english'))

seen_urls = set()
unique_urls = set()




def print_analytics():

    #MAKE UNIQUE PAGES
    print()
    print()
    unique_page_count = seen_urls
    print("=======================")
    print(f"Total unique pages found: {len(unique_page_count)}")
    print()





    longest_url = max(page_word_counts, key = page_word_counts.get)
    print(f"Longest page: {longest_url} ({page_word_counts[longest_url]} words)")
    print()



    # Top 50 words
    '''I DONT KNOW WHY ITS OKAY TO USE URL AND SOUP HERE IT DOESNT EXIST IN THIS FUNCITON ...'''
    common_words = count_words_from_page(url, soup)
    print("Top 50 most common words:")
    for word, count in common_words:
        print(f"{word}: {count}")
    print()


    # Subdomains
    print("Subdomains under uci.edu:")
    # print(subdomains)
    for domain in sorted(subdomains):
        print(f"{domain}: {subdomains[domain]}")
    print()







#gets the subdomains of each url that has been added to the queue
def extracting_subdomains(links):
    for url in links:
        #parsing url into parts
        parsed_url = urlparse(url)

        #if uci.edu in the string in the netloc part
        if "uci.edu" in parsed_url.netloc:
            #split uci.edu on the .
            domain_parts = parsed_url.netloc.split(".")

            if len(domain_parts) > 2:
                subdomain = domain_parts[0]
                subdomains[subdomain].add(url)

    sorted_subdomains = sorted(subdomains.items())

    result = [f"{subdomain}, {len(urls)}" for subdomain, urls in sorted_subdomains]
    print("SORTED SUBDOMAINS: ", result)
    return result




#TO FIND TOP 50 WORDS
#   LOOK INTO WHERE TO CALL THIS FOR REPORT
def count_words_from_page(url, soup):
    text = soup.get_text()
    words = re.findall(r'\w+', text.lower())

    # Filter out stop words
    filtered_words = [word for word in words if word not in stop_words]

    # Update the global word counter
    word_counter.update(filtered_words)

    # Store the word count for the current page (optional)
    page_word_counts[url] = len(filtered_words)

    # Return the top 50 most common words
    return word_counter.most_common(50)




def scraper(url, resp):
    links = extract_next_links(url, resp)
    valid_links = []

    #loop checking if every link in the unique urls have been crawled or not
    for link in links:
        # print("VALIDATING LINK")
        if is_valid(link) and link not in unique_urls:
            # print(link + " LINK IS VALID")
            #add each link to the unique url set
            unique_urls.add(link)
            #append the valid link to the valid links list
            valid_links.append(link)
    return valid_links





#   see if report needs to be about the sites actually crawled on the big crawl? or just the testing ones?
def find_longest_page():
    #if the dictionary is empty return false
    if not page_word_counts:
        return None
    #Gets the longest page in the dictionary
    longest_page = max(page_word_counts, key=page_word_counts.get)
    print(f"The longest page is: {longest_page} with {page_word_counts.get(longest_page)} words.")

    #rerutns the page itself (key) and the value
    return longest_page, page_word_counts[longest_page]






def is_unique_url(url):
    # Parse the URL and remove the fragment
    parsed_url = urlparse(url)
    base_url = url.split('#')[0]  # Remove fragment part

    # Check if the URL has been seen before
    if base_url in seen_urls:
        return False
    else:
        # If not, add it to the set
        seen_urls.add(base_url)
        return True






def extract_next_links(url, resp):
    print("URL: " + url, "RESP: " + str(resp))
    #extracts NEW LINKS FOUND INSIDE PAGE
    links = []
    if resp.status_code != 200 or not resp.content:
        print(resp.error)
        return links

    #LINK EXTRACTION LOGIC
    #soup is the HTML parser, makes it easier for us to look for the href 'a' stuff
    soup = BeautifulSoup(resp.content, 'html.parser')


    #FOR SEEING THE LONGEST PAGE
    #using soup to get the text of each url page
    text = soup.get_text()

    if len(text.strip()) < 50:
        print("Skipping low-information page:", url)
        return links


    #using soup to split the words of the url page
    words = re.findall(r'\w+', text.lower())
    #updates the longest page tracker
    page_word_counts[url] = len(words)


    #using soup to find the <a> tags
    print("about to go into for loop to defragment the urls")
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urljoin(url, href)
        #splits the url on the # and takes the first part
        defregmented_url = absolute_url.split('#')[0]
        if is_valid(defregmented_url) and is_unique_url(defregmented_url):
            links.append(defregmented_url)

        if len(links) > MAX_LINKS_PER_PAGE:
            print("Trap detected: too many links from", url)
            break

    extracting_subdomains(links)
    return links


def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)

        if parsed.scheme not in {'http', 'https'}:
            return False


        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        allowed_domains = [r".*\.ics\.uci\.edu.*",
            r".*\.cs\.uci\.edu.*",
            r".*\.informatics\.uci\.edu.*",
            r".*\.stat\.uci\.edu.*",
            r"today\.uci\.edu/department/information_computer_sciences/.*"]
        domain_matched = any(re.match(pattern, parsed.netloc) for pattern in allowed_domains)

        return domain_matched



    except TypeError:
        print ("TypeError for ", parsed)
        raise




if __name__ == "__main__":
    # seed_urls = ['https://jobs.uci.edu/careers-home/', 'https://ics.uci.edu/', 'https://uci.edu/']
    seed_urls = ['https://ics.uci.edu/']
    print("Starting crawl with seed URLs:", seed_urls)

    queue = deque(seed_urls)  # <- put your seeds into a queue

    MAX_PAGES_TO_CRAWL = 100  # safety limit for testing


    print(seen_urls)

    while queue and len(seen_urls) < MAX_PAGES_TO_CRAWL:
        url = queue.popleft()
        print(f"Processing: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()

            time.sleep(5)  # politeness

            soup = BeautifulSoup(response.content, 'html.parser')
            top_words = count_words_from_page(url, soup)
            print("Top 50 words from this page:")
            for word, count in top_words:
                print(f"{word}: {count}")


            valid_links = scraper(url, response)
            print(f"Valid links found: {valid_links}")

            queue.extend(valid_links)  # <- ADD new valid links into the queue


        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")

    print_analytics()
    # print(seen_urls)
