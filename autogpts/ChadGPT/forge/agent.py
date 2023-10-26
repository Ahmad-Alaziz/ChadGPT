import json
import asyncio
import pprint


from forge.sdk import (
    Agent,
    AgentDB,
    Step,
    StepRequestBody,
    Workspace,
    ForgeLogger,
    Task,
    TaskRequestBody,
    PromptEngine,
    chat_completion_request,
)

LOG = ForgeLogger(__name__)

from datetime import datetime


class JSONEncoderWithBytes(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
        # Add support for encoding datetime.datetime (or Timestamp) objects
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert to ISO formatted string
        return super().default(obj)
    
class ForgeAgent(Agent):
    MODEL_NAME = "gpt-3.5-turbo-16k"
    RETRY_COUNT = 3
    RETRY_WAIT_SECONDS = 5  # wait for 5 seconds before retrying

    def __init__(self, database: AgentDB, workspace: Workspace):
        super().__init__(database, workspace)

    async def create_task(self, task_request: TaskRequestBody) -> Task:
        task = await super().create_task(task_request)
        self.messages = [] 
        LOG.info(
            f"📦 Task created: {task.task_id} input: {task.input[:40]}{'...' if len(task.input) > 40 else ''}"
        )
        return task
    
    async def execute_step(self, task_id: str, step_request: StepRequestBody) -> Step:
        task = await self.db.get_task(task_id)
        step = await self.db.create_step(
            task_id=task_id, input=step_request, additional_input=step_request.additional_input, is_last=False
        )


        current_files = self.workspace.list(task_id, ".")

        if len(self.messages) < 2:
            prompt_engine = PromptEngine(self.MODEL_NAME)
            system_kwargs = {
                "abilities": self.abilities.list_abilities_for_prompt(),
                "current_files": current_files
            }
            task_kwargs = {"task": task.input}
            system_prompt = prompt_engine.load_prompt("system-format", **system_kwargs)
            self.messages = [{"role": "system", "content": system_prompt}]
            task_prompt = prompt_engine.load_prompt("task-step", **task_kwargs)
            self.messages.append({"role": "user", "content": task_prompt})

        LOG.debug(f"\n\n\nSending the following messages to the model: {pprint.pformat(self.messages)}")


        for retry_attempt in range(self.RETRY_COUNT):
            try:
                # Chat completion request
                chat_completion_kwargs = {
                    "messages": self.messages,
                    "model": self.MODEL_NAME,
                    "temperature": 0
                }
                chat_response = await chat_completion_request(**chat_completion_kwargs)

                answer_content = chat_response["choices"][0]["message"]["content"]

                # Check if the content is already a dictionary (JSON-like structure)
                if isinstance(answer_content, dict):
                    answer = answer_content
                else:
                    try:
                        # If answer_content is bytes, decode it
                        if isinstance(answer_content, bytes):
                            answer_content = answer_content.decode('utf-8')
                        
                        # Attempt to parse the content as JSON
                        answer = json.loads(answer_content)
                        LOG.debug(f"\n\n\nanswer: {pprint.pformat(answer)}")

                    except json.JSONDecodeError:
                        LOG.error(f"Unable to decode chat response: {chat_response}")
                        answer = None

                # Ability Sequence Execution
                ability_sequence = answer.get("abilities_sequence")
                previous_output = None

                for ability_item in ability_sequence:
                    ability = ability_item.get("ability", {})
                    LOG.debug("\n\nin the sequence %s", ability)

                    if "name" in ability and "args" in ability:
                        if previous_output and ability["name"] != "finish":
                            ability["args"].update({"input": previous_output})

                        output = await self.abilities.run_ability(
                            task_id, ability["name"], **ability["args"]
                        )

                        LOG.debug("\n\nGot Output for %s : %s", ability["name"], output)

                        if ability["name"] == "finish" or "File has been written successfully" in output:
                            step.is_last = True
                            step.status = "completed"

                        step.output = answer
                        previous_output = output
                    else:
                        step.output = answer.get("speak", answer)

                if previous_output and isinstance(previous_output, str):
                    answer["final_output"] = previous_output

                # If everything is successful, break out of the retry loop
                LOG.info("\n\aanswer final %s", answer)
                break
            

            except Exception as e:
                if retry_attempt < self.RETRY_COUNT - 1:
                    LOG.warning(f"Error occurred in attempt {retry_attempt + 1}. {str(e)}")
                    await asyncio.sleep(self.RETRY_WAIT_SECONDS)
                else:
                    LOG.error(f"Error occurred in the final attempt {retry_attempt + 1}. Giving up.")
                    raise

        stringified_answer = json.dumps(answer, cls=JSONEncoderWithBytes)
        self.messages.append({"role": "assistant", "content": stringified_answer})

        if len(self.messages) >= 4:
            step.is_last = True

        return step
