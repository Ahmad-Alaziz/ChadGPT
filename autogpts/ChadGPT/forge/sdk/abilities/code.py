# from typing import Any
# from ..forge_log import ForgeLogger

# from .registry import ability
# from ..llm import chat_completion_request
# import re

# logger = ForgeLogger(__name__)

# @ability(
#     name="generate_code",
#     description="Generate python code for a detailed description of the task, it should only be used when the user has specifically asked for code generation",
#     parameters=[
#         {
#             "name": "input",
#             "description": "Detailed description of the task for which code needs to be generated",
#             "type": "string",
#             "required": True,
#         },
#     ],
#     output_type="string",
# )

# async def generate_code(agent, task_id: str, input: str) -> str:
#     """
#     Generate code for a specific task.
#     """
#     # Initialize messages with the system message
#     messages = [{
#         "role": "system",
#         "content": f"You are a senior programmer. Write python code for the following task: {input}. Make sure to just return the code without any added information or explanation. You should always return the full code. Your response needs to be executable code, I will take the response you give and execute it immediately. If you make a mistake and correct yourself, don't apologize or explain yourself, just return the corrected code. If the user sends you an error message, but you suspect that the issue is with the way that the user executed the code, and not a problem with the code itself, then you can simply return the exact same code with no changes."
#     }]

#     try:
#         response = await chat_completion_request(
#             messages=messages,
#             model="gpt-3.5-turbo"
#         )

#         generated_code = strip_markdown_blocks(response["choices"][0]["message"]["content"])
        
#         # Append assistant's response to messages
#         messages.append({
#             "role": "assistant",
#             "content": generated_code
#         })
        
#         logger.info("Generated code: %s", generated_code)

#         # If filename is provided, write the code to the file
#         # if filename:
#         #     await write_file(agent, task_id, filename, generated_code)

#     except Exception as e:
#         error_message = str(e)
#         # Limit error message to 2500 characters
#         error_message = error_message[:2500]
        
#         # Append user's error message to messages
#         messages.append({
#             "role": "user",
#             "content": error_message
#         })
        
#         logger.error(f"Error generating code: {e}")

#     return generated_code

# async def write_file(agent, task_id: str, file_path: str, input: bytes) -> None:
#     """
#     Write data to a file
#     """
#     if isinstance(input, str):
#         input = input.encode()

#     agent.workspace.write(task_id=task_id, path=file_path, data=input)
#     await agent.db.create_artifact(
#         task_id=task_id,
#         file_name=file_path.split("/")[-1],
#         relative_path=file_path,
#         agent_created=True,
#     )

# def strip_markdown_blocks(code: str) -> str:
#     """Strip markdown code blocks annotations from the provided code."""
#     pattern = r"```(.*?)```"
#     matches = re.findall(pattern, code, re.DOTALL)
    
#     if matches:
#         # Return the first match without any surrounding whitespace
#         return matches[0].strip()
    
#     # If no markdown blocks found, return the original code
#     return code
