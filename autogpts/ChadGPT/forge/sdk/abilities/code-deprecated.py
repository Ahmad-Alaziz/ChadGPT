
# from typing import  Any
# from ..forge_log import ForgeLogger
# from .registry import ability
# from ..llm import chat_completion_request
# import re



# logger = ForgeLogger(__name__)


# @ability(
#     name="generate_code",
#     description="Generate python code for a detailed description of the task",
#     parameters=[
#         {
#             "name": "input",
#             "description": "Detailed description of the task for which code needs to be generated",
#             "type": "string",
#             "required": True,
#         },
#         {
#             "name": "filename",
#             "description": "Optional filename to write the generated code to",
#             "type": "string",
#             "required": False,
#         }
#     ],
#     output_type="string",
# )

# async def generate_code(agent, task_id: str, input: str, filename: str = None) -> str:
#     """
#     Generate code for a specific task and verify its correctness using GPT-3.5-turbo
#     """
#     max_attempts = 3
    
#     # Initialize messages with the system message
#     messages = [{
#         "role": "system",
#         "content": f"You are a senior programmer. Write python code for the following task: {input}. Make sure to just return the code without any added information or explanation.You should always return the full code. Your response needs to be executable code, I will take the response you give and execute it immedietly. If you make a mistake and correct yourself, dont appologize or explain yourself, just return the corrected code. If the user send you an error message, but you suspect that the issue is with the way that the user executed the code, and not a problem with the code itself, then you can simply return the exact saame code with no changes"
#     }]

#     for attempt in range(max_attempts):
#         try:
#             response = await chat_completion_request(
#                 messages=messages,
#                 model="gpt-3.5-turbo"
#             )

#             generated_code =  strip_markdown_blocks(response["choices"][0]["message"]["content"])
            
#             # Append assistant's response to messages
#             messages.append({
#                 "role": "assistant",
#                 "content": generated_code
#             })
            
#             logger.info("Generated code: %s", generated_code)

#             # Execute the generated code and get its output
#             exec_output = await execute_code(generated_code)

#             verification_response = await chat_completion_request(
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": f"Verify the following python code for the task '{input}'. if the output is correct, simply return correct.\nCode: {generated_code}\nOutput: {exec_output}"
#                     }
#                 ],
#                 model="gpt-3.5-turbo"
#             )

#             verification_result = verification_response["choices"][0]["message"]["content"].strip()
#             logger.info("verification_result: %s", verification_result)

#             if "correct" in verification_result.lower():
#                 # If filename is provided, write the code to the file
#                 if filename:
#                     await write_file(agent, task_id, filename, generated_code)

#                 return generated_code

#         except Exception as e:
#             error_message = str(e)
            
#             # Limit error message to 2500 characters
#             error_message = error_message[:2500]
            
#             # Append user's error message to messages
#             messages.append({
#                 "role": "user",
#                 "content": error_message
#             })
            
#             logger.error(f"Error in attempt {attempt + 1}: {e}")

#     if filename:
#         await write_file(agent, task_id, filename, generated_code)

#     return generated_code

# def execute_code(code: str) -> Any:
#     """
#     Execute the provided Python code and return its output.
#     This function attempts to run code in a somewhat controlled manner, but ensure
#     further sandboxing for production use.
#     """
#     # Using contextlib for stdout and stderr redirection
#     from contextlib import redirect_stdout, redirect_stderr
#     from io import StringIO

#     result = None
#     redirected_output = StringIO()
#     with redirect_stdout(redirected_output), redirect_stderr(redirected_output):
#         try:
#             # Use a dictionary to capture local variables after execution
#             local_vars = {}
#             exec(code, {}, local_vars)
#             result = local_vars.get('result', None)
#         except Exception as e:
#             logger.error(f"Error executing code: {e}")

#     # Return the captured output
#     output = redirected_output.getvalue()
#     redirected_output.close()
#     return output or result

# async def write_file(agent, task_id: str, file_path: str, input: bytes) -> None:
#     """
#     Write data to a file
#     """
#     if isinstance(input, str):  # Fixed from 'data' to 'input'
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