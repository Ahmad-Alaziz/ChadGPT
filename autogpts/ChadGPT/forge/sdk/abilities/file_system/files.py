from typing import List
import os
import json

from ..registry import ability
from ...forge_log import ForgeLogger


logger = ForgeLogger(__name__)


@ability(
    name="list_files",
    description="List files in a directory",
    parameters=[
        {
            "name": "input",
            "description": "Path to the directory",
            "type": "string",
            "required": True,
        }
    ],
    output_type="list[str]",
)
async def list_files(agent, task_id: str, input: str) -> List[str]:
    """
    List files in a workspace directory
    """
    return agent.workspace.list(task_id=task_id, path=str(input))


@ability(
    name="write_file",
    description="Write data to a file",
    parameters=[
        {
            "name": "file_path",
            "description": "Path to the file",
            "type": "string",
            "required": True,
        },
        {
            "name": "input",
            "description": "Input data to write to the file in bytes",
            "type": "bytes",
            "required": True,
        },
    ],
    output_type="None",
)
async def write_file(agent, task_id: str, file_path: str, input: bytes):
    """
    Write data to a file
    """
    if isinstance(input, str):
        input = input.encode()
    elif isinstance(input, dict):
        input = json.dumps(input).encode()
    elif not isinstance(input, bytes):
     raise TypeError(f"Unsupported input type: {type(input)}")

    agent.workspace.write(task_id=task_id, path=file_path, data=input)
    created_artifact = await agent.db.create_artifact(
        task_id=task_id,
        file_name=file_path.split("/")[-1],
        relative_path=file_path,
        agent_created=True,
    )

    logger.info(f"Artifact created in write file: {created_artifact.dict()}")
    
    return "File has been written successfully to " + file_path

@ability(
    name="read_file",
    description="Read data from a file",
    parameters=[
        {
            "name": "file_path",
            "description": "Path to the file",
            "type": "string",
            "required": True,
        },
        {
            "name": "input",
            "description": "Optional input filename to use instead of file_path",
            "type": "string",
            "required": False,
        },
    ],
    output_type="bytes",
)
async def read_file(agent, task_id: str, file_path: str, input: str = None) -> bytes:
    """
    Read data from a file
    """
    path_to_use = input if input and isinstance(input, str) and os.path.isfile(input) else file_path
    return agent.workspace.read(task_id=task_id, path=path_to_use)


@ability(
    name="read_multiple_files",
    description="Read data from multiple files",
    parameters=[
        {
            "name": "input",
            "description": "List of paths to the files",
            "type": "list[str]",
            "required": True,
        }
    ],
    output_type="dict[str, bytes]",
)
async def read_multiple_files(agent, task_id: str, input: List[str]) -> dict:
    """
    Read data from multiple files and return as a dictionary with file names as keys and data as values.
    """
    data = {}
    for path in input:
        data[path.split("/")[-1]] = agent.workspace.read(task_id=task_id, path=path)
    return data
