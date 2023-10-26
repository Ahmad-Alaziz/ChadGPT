from ..forge_log import ForgeLogger
from ..llm import chat_completion_request
from .registry import ability
import json

logger = ForgeLogger(__name__)

@ability(
    name="process_data",
    description="Process data based on a specified transformation description, make sure to pass a very detailed description of the desired transformation. You can also pass multiple transformation commands. You must give examples of the desired outcome in the description.",
    parameters=[
        {
            "name": "transformation_description",
            "description": "Detailed Description of the transformation to be applied",
            "type": "string",
            "required": True,
        },
        {
            "name": "input",
            "description": "The data to be transformed",
            "type": "string",
            "required": True,
        }
    ],
    output_type="string",
)
async def process_data(agent, task_id: str, transformation_description: str, input: str) -> str:
    # Determine the type of input_data
    if isinstance(input, bytes):
        input_str = input.decode('utf-8')
    elif isinstance(input, dict):
        input_str = json.dumps(input)
    else:
        input_str = str(input)
        
    # Check input length for direct transformation
    if len(input_str) < 4000:
        try:
            transformed_data = await direct_transformation_request(transformation_description, input_str)
            return transformed_data
        except Exception as direct_e:
            direct_error_message = str(direct_e)
            logger.warning(f"Direct transformation error: {direct_error_message}")
            return json.dumps({"error": direct_error_message})

    # If input length >= 4000, continue with the current logic
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            transformation_function_code = await generate_transformation_function(transformation_description, attempt, error_message=None if attempt == 0 else error_message)
            
            exec_globals = {}
            exec(transformation_function_code, {}, exec_globals)
            transformation_function = exec_globals['transformation_function']
            
            input_obj = json.loads(input_str)
            transformed_data_obj = transformation_function(input_obj)
            
            transformed_data = json.dumps(transformed_data_obj)
            return transformed_data

        except Exception as e:
            error_message = str(e)
            logger.warning(f"Attempt {attempt + 1}: Error processing data: {error_message}")

            # On the last attempt, directly ask the model for the transformation result
            if attempt == max_attempts - 1:
                try:
                    transformed_data = await direct_transformation_request(transformation_description, input_str)
                    return transformed_data
                except Exception as final_e:
                    final_error_message = str(final_e)
                    logger.warning(f"Final attempt: Error processing data: {final_error_message}")
                    return json.dumps({"error": final_error_message})



async def generate_transformation_function(description: str, attempt: int, error_message: str = None) -> str:
    messages = [{
        "role": "system",
        "content": f"You are a data transformation expert. Generate a Python function named 'transformation_function' to perform the following transformation: {description}. SImply return the code and nothing more!"
    }]
    
    if error_message:
        messages.append({
            "role": "user",
            "content": f"Previous error: {error_message} (Attempt {attempt + 1})"
        })

    try:
        response = await chat_completion_request(
            messages=messages,
            model="gpt-3.5-turbo"
        )

        transformation_function_code = response["choices"][0]["message"]["content"].strip()
        # Adding the model's response to the message list
        messages.append({
            "role": "assistant",
            "content": transformation_function_code
        })

        return transformation_function_code

    except Exception as e:
        error_message = str(e)
        logger.warning(f"Error generating transformation function: {error_message}")
        return f"def transformation_function(data): raise Exception('Failed to generate transformation function: {error_message}')"

async def direct_transformation_request(description: str, input_str: str) -> str:
    messages = [
        {
            "role": "system",
            "content": f"You are a data transformation expert. Given the data '{input_str}', apply the following transformation: {description} and return the result. Simply return the result of the transformation without any explanation or added talk. If the result is already in the form of the transformation description, then you do not need to change anything, return the same result."
        }
    ]
    
    response = await chat_completion_request(
        messages=messages,
        model="gpt-3.5-turbo"
    )
    
    transformed_data = response["choices"][0]["message"]["content"].strip()
    return transformed_data
