from fuzzywuzzy import fuzz
from .registry import ability
from ..llm import chat_completion_request
import re

@ability(
    name="find_in_text",
    description="Fuzzy search given input using a search_term",
    parameters=[
        {
            "name": "input",
            "description": "Text to search within",
            "type": "string",
            "required": True,
        },
        {
            "name": "search_term",
            "description": "Term to search for within the input text",
            "type": "string",
            "required": True,
        }
    ],
    output_type="string",
)
async def find_in_text(agent, task_id: str, input: str = "", search_term: str = "") -> str:

    if isinstance(input, dict):
        # This is a simple conversion, you might want to customize it based on your needs
        input = '\n'.join([f"{k}: {v}" for k, v in input.items()])

    try:
        # Split the text into sentences
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', input)

        # Use fuzz.partial_ratio to get sentences that likely contain the search term
        threshold = 80
        relevant_sentences = [sentence.strip() for sentence in sentences if fuzz.partial_ratio(search_term.lower(), sentence.lower()) >= threshold]
        
        # Return the matched sentences if found
        if relevant_sentences:
            return "\n".join(relevant_sentences)
        
        # If no relevant sentences found, use the gpt-3.5-model
        messages = [{
            "role": "system",
            "content": f"You are a content analyzer. Find and return parts of the text that are most relevant to the search term: '{search_term}'. The text you need to analyze is: '{input}'"
        }]

        response = await chat_completion_request(
            messages=messages,
            model="gpt-3.5-turbo"
        )

        model_response = response["choices"][0]["message"]["content"]
        
        if model_response.strip():
            return model_response
        else:
            return "Nothing relevant was found."

    except Exception as e:
        return str(e)


