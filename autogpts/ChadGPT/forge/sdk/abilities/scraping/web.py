import asyncio
from itertools import islice
import trafilatura
from googlesearch import search
from duckduckgo_search import DDGS
from aiohttp import ClientSession, TCPConnector

from ..registry import ability
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from ...llm import chat_completion_request
import json


import re

from ...forge_log import ForgeLogger
from .web_selenium import search_internet_with_selenium, is_valid_url
logger = ForgeLogger(__name__)


# Constants
NUM_RESULTS = 3
DUCKDUCKGO_MAX_ATTEMPTS = 3
EXTRACT_LIMIT = 4500

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "DNT": "1",  # Do Not Track Request Header
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}



# # A simple URL validation function
# def is_valid_url(url):
#     # Basic regex for URL validation
#     regex = re.compile(
#         r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
#     )
#     return regex.match(url)

async def fetch_with_duckduckgo(query: str, num_results: int) -> list:
    search_results = []
    attempts = 0

    while attempts < DUCKDUCKGO_MAX_ATTEMPTS:
        if not query:
            return []

        results = DDGS().text(query)
        if results:
            search_results = [result['href'] for result in islice(results, num_results)]
            logger.debug("DuckDuckGo fetched %d URLs (num_results was set to %d)", len(search_results), num_results)

        if search_results:
            break

        await asyncio.sleep(2 ** attempts)  # Exponential backoff
        attempts += 1

    return search_results


async def fetch_with_google(query: str, num_results: int) -> list:
    urls = list(search(query, num_results=num_results, lang="en"))
    logger.debug("Google fetched %d URLs (num_results was set to %d)", len(urls), num_results)
    return urls


def extract_top_relevant_info(clean_text, input_terms):
    sentences = [s for s in clean_text.split('.') if s]

    # Use TF-IDF to vectorize the sentences
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(sentences)
    query_vector = vectorizer.transform([' '.join(input_terms)])

    # Compute cosine similarity between the query and all sentences
    cosine_similarities = cosine_similarity(query_vector, tfidf_matrix).flatten()
    
    # Rank sentences based on their cosine similarity
    ranked_sentences = [sentences[i] for i in cosine_similarities.argsort()[::-1]]

    return '. '.join(ranked_sentences[:8]) if ranked_sentences else ""


@ability(
    name="fetch_webpage",
    description="Fetch and extract content from a given a valid webpage URL, takes url as input, and outputs string of data. Do not use example or fake urls",
    parameters=[
        {
            "name": "input",
            "description": "Webpage URL, only use if the url is valid",
            "type": "string",
            "required": True,
        }
    ],
    output_type="string",
)
async def fetch_webpage_data(agent, task_id: str, input: str) -> str:
    if not is_valid_url(input):
        logger.error(f"Invalid URL provided: {input}")
        return ""

    connector = TCPConnector(limit_per_host=3)
    async with ClientSession(headers=HEADERS, connector=connector) as session:
        async with session.get(input, timeout=60) as response:
            if response.status != 200:
                return ""

            try:
                html = await response.text()
            except UnicodeDecodeError:
                try:
                    html = await response.text(encoding='iso-8859-1')
                except:
                    logger.error("Failed to decode the content from URL: %s", input)
                    return ""

            return trafilatura.extract(html)
        

async def extract_info_from_urls(agent, task_id: str, urls: list, query: str) -> str:
    logger.info("Extracting info from urls: %s", urls)

    error_message = "Please enable JavaScript or switch to a supported browser to continue using twitter."
    input_terms = query.split()
    aggregated_info = []

    for url in urls:
        data = await fetch_webpage_data(agent, task_id, url)
        
        # If fetch_webpage_data fails to retrieve the data or retrieves the error message, use Selenium
        if not data or error_message in data:
            logger.warn(f"Failed to fetch from {url} using fetch_webpage_data or detected error message. Trying with Selenium...")
            data = await search_internet_with_selenium(agent, task_id, url, query)
        
        if data and error_message not in data:
            info = extract_top_relevant_info(data, input_terms)
            if info:
                aggregated_info.append(info)

    result = ' '.join(aggregated_info[:8])[:EXTRACT_LIMIT]
    logger.warn(f"\n\nExtracting results: {result}")

    return result


@ability(
    name="search_internet",
    description="Search for a term and retrieve relevant content, takes a search query as input, and outputs string of data. You must imput a detailed search query not a vague one. Be as specific as possible! Add any additional context that might help with the search",
    parameters=[
        {
            "name": "input",
            "description": "Search query that uses astricks.",
            "type": "string",
            "required": True,
        }
    ],
    output_type="string",
)
async def search_internet(agent, task_id: str, input: str) -> str:
    enhanced_query = await enhance_query_with_gpt(input)
    logger.info(f"old query: {input}, enhanced query: {enhanced_query}")

    input_terms = enhanced_query.split()

    urls = []
    for _ in range(2):  # maximum of two times loop
        try:
            # First, try with Google
            urls = await fetch_with_google(input, NUM_RESULTS)

        except Exception as e:
            logger.error(f"Google search failed with error: {e}")
            urls = []

        if not urls:
            # Fallback to google if duckduck search failed
            urls = await fetch_with_duckduckgo(input, NUM_RESULTS)


        # Extract information from the obtained URLs
        extracted_info = await extract_info_from_urls(agent, task_id, urls, input)

        
        if not extracted_info:
            return "Couldn't extract specific information from any of the fetched content."
        
        feedback_response = await check_with_gpt(extracted_info, input)
        if feedback_response["action"] == "return":
            return feedback_response["content"]
        elif feedback_response["action"] == "new_query":
            input = feedback_response["content"]
        # else:
            # return "An error occurred while verifying the information."

        # Call the Selenium function when no valid answer is found after two attempts.
    # Assuming urls is a list of URLs you want to pass.
    for url in urls:
        response = await search_internet_with_selenium(agent, task_id, url, input)
        if response:
            feedback_response = await check_with_gpt(response, input)
            if feedback_response["action"] == "return":
                return feedback_response["content"]

    return "Failed to find a suitable answer even with Selenium."



async def check_with_gpt(extracted_info: str, original_query: str) -> dict:
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert search verifier. Your role is to verify if the result of a search query from the web contains a valid answer.\n\n"
                "A valid response is one that either 1) correctly answers the users question for example: Query: Who is the ceo of tesla: Answer: Elon Musk. 2) contains the valid answer within the response eg.: Elon Musk, the CEO of Tesla and SpaceX, has always been known for his unpredictable nature and his ability to make headlines\n\n"

                "## Response Format\n"
                "Respond only in one of the following format templates \n\n"

                "1- if the content contains valid answer that you beleive is accecpeted use this template:\n"
                "{\n"
                "    \"accepted\": true,\n"
                "    \"formattedAnswer\": \" reformatted answer for clarity that excludes content which the user did not search for\"\n"
                "}\n\n"
                "2- if the content does not contain valid answer, respond with this template, the query key should have an actual valid query ready to be searched again:\n"
                "{\n"
                "    \"accepted\": false,\n"
                "    \"query\": \"New search query that user will use again to try and scrape the answer of the web, remember to utilize techniques like using the asterisk (*) as a wildcard. which can be highly effective\"\n"
                "}"
                "Do not add any of your own thoughts or talk, simply respond with the intended format with the right content."
            )
        },
         {
            "role": "user",
            "content": (
                f"Here is the Query: '{original_query}'. \n"
                f"Here is the answer we received: {extracted_info}\n\n"
                f"Do you think the answer is a good enough response to the above query?"
            )
        }
    ]

    try:
        response = await chat_completion_request(
            messages=messages,
            model="gpt-3.5-turbo"
        )

        response_content = response["choices"][0]["message"]["content"]
        logger.debug(f"response_content: {response_content}")

        
        # Parse the content into a JSON dictionary
        response_json = json.loads(response_content)

        # Handle the response based on the provided format
        if response_json["accepted"] is True:
            logger.info(f"GPT provided a reformatted answer. {response_json['formattedAnswer']}")
            return {"action": "return", "content": response_json["formattedAnswer"]}
        elif response_json["accepted"] is False:
            logger.info(f"GPT suggests a new query: {response_json['query']}")
            return {"action": "new_query", "content": response_json["query"]}
        else:
            return {"action": "error", "content": "Unrecognized response format from GPT."}

    except Exception as e:
        logger.error(f"Error while checking with GPT: {e}")
        return {"action": "error", "content": ""}



async def enhance_query_with_gpt(query: str) -> str:
    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful assistant specialized in refining search queries to ensure "
                "they are optimized for fetching precise information from the web. "
                "Your task is to expand or adjust the provided query to make it more effective. Respond with the newly modified query and nothing more."
                "Remember about the ability to use advanced techniques like astricks as wildcards"
            )
        },
        {
            "role": "user",
            "content": query
        }
    ]

    try:
        response = await chat_completion_request(
            messages=messages,
            model="gpt-3.5-turbo"
        )

        enhanced_query = response["choices"][0]["message"]["content"].strip()
        return enhanced_query

    except Exception as e:
        logger.error(f"Error while enhancing query with GPT: {e}")
        return query  # return the original query in case of an error

