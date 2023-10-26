from datetime import datetime
from .registry import ability

@ability(
    name="get_current_time",
    description="Return the current date and time",
    parameters=[   {
            "name": "input",
            "description": "optional placeholder",
            "type": "string",
            "required": False,
        }],
    output_type="string")

async def get_datetime(agent, task_id, input) -> str:
    return "Current date and time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
