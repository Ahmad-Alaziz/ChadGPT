from bs4 import BeautifulSoup
from ...forge_log import ForgeLogger
from ..registry import ability
import requests

@ability(
    name="fetch_and_search",
    description="Fetch a webpage and return content relevant to a specified search term as a single string",
    parameters=[
        {
            "name": "url",
            "description": "Webpage URL",
            "type": "string",
            "required": True,
        },
        {
            "name": "search_term",
            "description": "Term to search for within the fetched webpage",
            "type": "string",
            "required": True,
        }
    ],
    output_type="string",
)
async def fetch_and_search(agent, task_id: str, url: str, search_term: str) -> str:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        body_text = soup.get_text()

        # Split the text into lines, search for the term, and concatenate the lines
        relevant_lines = [line.strip() for line in body_text.splitlines() if search_term.lower() in line.lower()]
        result_string = "\n".join(relevant_lines)

        return result_string

    except requests.RequestException as e:
        ForgeLogger.error(f"Failed to fetch or parse {url} due to {str(e)}")
        return ""


@ability(
  name="fetch_webpage",
  description="Retrieve the content of a webpage",
  parameters=[
      {
          "name": "url",
          "description": "Webpage URL",
          "type": "string",
          "required": True,
      }
  ],
  output_type="string",
)
async def fetch_webpage(agent, task_id: str, url: str) -> str:
  response = requests.get(url)
  return response.text