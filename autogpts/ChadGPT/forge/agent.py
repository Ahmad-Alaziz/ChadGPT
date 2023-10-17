import json
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


class ForgeAgent(Agent):

    def __init__(self, database: AgentDB, workspace: Workspace):
        super().__init__(database, workspace)

    def safe_json_dumps(self, data):
        """
        Convert data into a JSON string, ensuring bytes are decoded.
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, bytes):
                    data[key] = value.decode('utf-8')
        elif isinstance(data, bytes):
            data = data.decode('utf-8')
        
        return json.dumps(data)


    async def create_task(self, task_request: TaskRequestBody) -> Task:
        task = await super().create_task(task_request)
        self.messages = [] 

        LOG.info(
            f"📦 Task created: {task.task_id} input: {task.input[:40]}{'...' if len(task.input) > 40 else ''}"
        )
        return task
    

    async def execute_step(self, task_id: str, step_request: StepRequestBody) -> Step:
        task = await self.db.get_task(task_id)
        print(f"\n\n\ntask: {task} step request: {step_request}")

        
        step = await self.db.create_step(
            task_id=task_id, input=step_request, additional_input=step_request.additional_input, is_last=False
        )

        if len(self.messages) < 2:
            # Create a new step in the database

            # Log the message

            # Initialize the PromptEngine with the "gpt-3.5-turbo" model
            prompt_engine = PromptEngine("gpt-3.5-turbo")

            # Load the system and task prompts
            system_prompt = prompt_engine.load_prompt("system-format")

            # Initialize the messages list with the system prompt
            self.messages = [{"role": "system", "content": system_prompt}]

            # Define the task parameters
            task_kwargs = {
                "task": task.input,
                "abilities": self.abilities.list_abilities_for_prompt(),
            }

            # Load the task prompt with the defined task parameters
            task_prompt = prompt_engine.load_prompt("task-step", **task_kwargs)
            # Append the task prompt to the messages list
            self.messages.append({"role": "user", "content": task_prompt})


    

        LOG.debug(f"\n\n\nSending the following messages to the model: {pprint.pformat(self.messages)}")

        answer = None
        try:
            # Define the parameters for the chat completion request
            chat_completion_kwargs = {
                "messages": self.messages,
                "model": "gpt-3.5-turbo",
            }

            # Make the chat completion request and parse the response
            chat_response = await chat_completion_request(**chat_completion_kwargs)
            answer_content = chat_response["choices"][0]["message"]["content"]
            if isinstance(answer_content, bytes):
                answer_content = answer_content.decode('utf-8')
            answer = json.loads(answer_content)
           
            # Log the answer for debugging purposes
            LOG.debug(f"\n\n\nanswer: {pprint.pformat(answer)}")


        except json.JSONDecodeError:
            # Handle JSON decoding errors
            LOG.error(f"Unable to decode chat response: {chat_response}")
        except Exception as e:
            # Handle other exceptions
            LOG.error(f"Unable to generate chat response: {e}")

        if not answer:
            LOG.error("Chat completion did not return a valid answer.")
            # Handle this scenario - either return or set a default value for 'ability'.
            return step

        # Extract the ability from the answer
        ability = answer.get("ability", {})

        if "name" in ability and "args" in ability:
            # Run the ability and get the output
            output = await self.abilities.run_ability(
                task_id, ability["name"], **ability["args"]
            )

            print("\n\n\noutput legit: ", output)

            if ability["name"] == "finish":
                step.is_last = True
                step.status = "completed"

            answer["output"] = output

            # Set the step output to the "speak" part of the answer
            step.output = answer

     
            stringified_answer = self.safe_json_dumps(answer)
            self.messages.append({"role": "assistant", "content": stringified_answer})


        else:
            # Set the step output to the answer
            step.output = answer
            stringified_answer = self.safe_json_dumps(answer)
            self.messages.append({"role": "assistant", "content": stringified_answer})


        if len(self.messages) >= 5:
            step.is_last = True

        # Return the completed step
        return step
