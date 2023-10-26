import csv
from io import StringIO  # Make sure to import StringIO
from typing import Dict, List, Union


from ..registry import ability
from ...forge_log import ForgeLogger


logger = ForgeLogger(__name__)


# @ability(
#     name="read_csv",
#     description="Read data from a CSV file",
#     parameters=[
#         {
#             "name": "input",
#             "description": "Path to the CSV file",
#             "type": "string",
#             "required": True,
#         }
#     ],
#     output_type="List[Dict[str, Union[str, int, float]]]",
# )
# async def read_csv(agent, task_id: str, input: str) -> List[Dict[str, Union[str, int, float]]]:
#     data = agent.workspace.read(task_id=task_id, path=input).decode()
#     reader = csv.DictReader(data.splitlines())
#     return list(reader)

# @ability(
#     name="write_csv",
#     description="Write data to a CSV file",
#     parameters=[
#         {
#             "name": "file_path",
#             "description": "Path to the CSV file",
#             "type": "string",
#             "required": True,
#         },
#         {
#             "name": "input",
#             "description": "Data to be written in the CSV format",
#             "type": "List[Dict[str, Union[str, int, float]]]",
#             "required": True,
#         }
#     ],
#     output_type="None",
# )
# async def write_csv(agent, task_id: str, file_path: str, input: List[Dict[str, Union[str, int, float]]]):
#     lines = []
#     if input:
#         headers = input[0].keys()
#         writer = csv.DictWriter(lines, fieldnames=headers)
#         writer.writeheader()
#         for row in input:
#             writer.writerow(row)
    
#     csv_content = "\n".join(lines)
#     agent.workspace.write(task_id=task_id, path=file_path, data=csv_content.encode())


@ability(
    name="sort_csv",
    description="Sort CSV data",
    parameters=[
        {
            "name": "input",
            "description": "Data in CSV format (as bytes) to be sorted",
            "type": "bytes",
            "required": True,
        },
        {
            "name": "column",
            "description": "Column to sort by",
            "type": "string",
            "required": True,
        }
    ],
    output_type="bytes",  # Change the output type to bytes
)
async def sort_csv(agent, task_id: str, input: bytes, column: str) -> bytes:
    # Decode the bytes and read the data
    data_str = input.decode('utf-8')
    reader = csv.DictReader(StringIO(data_str))
    data_list = list(reader)
    
    sorted_data = sorted(data_list, key=lambda x: x.get(column))
    
    # Convert the sorted data back to CSV string
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=sorted_data[0].keys())
    writer.writeheader()
    for row in sorted_data:
        writer.writerow(row)
    
    # Return the sorted data as bytes
    return output.getvalue().encode('utf-8')

# @ability(
#     name="modify_csv",
#     description="Modify CSV data",
#     parameters=[
#         {
#             "name": "input",
#             "description": "Data in CSV format to be modified",
#             "type": "List[Dict[str, Union[str, int, float]]]",
#             "required": True,
#         },
#         {
#             "name": "action",
#             "description": "Type of modification e.g., 'add_column'",
#             "type": "string",
#             "required": True,
#         },
#         {
#             "name": "details",
#             "description": "Details of the modification e.g., column name and values for 'add_column'",
#             "type": "Dict[str, any]",
#             "required": True,
#         }
#     ],
#     output_type="List[Dict[str, Union[str, int, float]]]",
# )
# async def modify_csv(agent,  task_id: str, input: List[Dict[str, Union[str, int, float]]], action: str, details: Dict[str, any]) -> List[Dict[str, Union[str, int, float]]]:
#     if action == 'add_column':
#         column_name = details.get('column_name')
#         values = details.get('values', [])
#         for i, row in enumerate(input):
#             row[column_name] = values[i] if i < len(values) else ""
#     # Other modifications can be added similarly
#     return input

