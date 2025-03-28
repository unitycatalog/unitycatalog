import os
import asyncio
from dotenv import load_dotenv
from unitycatalog.ai.core.databricks import DatabricksFunctionClient
from unitycatalog.ai.semantic_kernel.toolkit import UCFunctionToolkit
from semantic_kernel import Kernel
from semantic_kernel.connectors.ai.open_ai import OpenAIChatCompletion
from semantic_kernel.connectors.ai.function_choice_behavior import FunctionChoiceBehavior
from semantic_kernel.connectors.ai.prompt_execution_settings import PromptExecutionSettings
from semantic_kernel.contents.chat_history import ChatHistory

# Load environment variables from .env file
load_dotenv()


def setup_uc_client():
    """Set up the Unity Catalog client."""
    # Configure the Unity Catalog API client

    # Instantiate the UnitycatalogFunctionClient
    return DatabricksFunctionClient(profile='e2-dogfood')


def create_calculator_function(uc_client, catalog, schema):
    """Create a calculator function in Unity Catalog."""
    def add_numbers(a: float, b: float) -> float:
        """
        Adds two numbers and returns the result.

        Args:
            a (float): First number.
            b (float): Second number.

        Returns:
            float: The sum of the two numbers.
        """
        return a + b

    # Create the function within the Unity Catalog catalog and schema specified
    function_info = uc_client.create_python_function(
        func=add_numbers,
        catalog=catalog,
        schema=schema,
        replace=True,  # Set to True to overwrite if the function already exists
    )
    return function_info


def setup_semantic_kernel():
    """Set up the Semantic Kernel with OpenAI integration and settings."""
    # Initialize the kernel with an AI service
    kernel = Kernel()

    # Add chat completion service
    chat_completion_service = OpenAIChatCompletion(
        ai_model_id='gpt-4',
        api_key=os.getenv("OPENAI_API_KEY")
    )

    # Set up execution settings
    settings = PromptExecutionSettings(
        function_choice_behavior=FunctionChoiceBehavior.Auto(),
    )

    return kernel, chat_completion_service, settings


def create_chat_history():
    """Create and initialize chat history."""
    chat_history = ChatHistory()
    chat_history.add_user_message(
        """You are a helpful calculator assistant. Use the calculator tools to answer questions about numbers.
        Question: What is 49 + 82?"""
    )
    return chat_history


async def process_chat(chat_history, chat_completion_service, settings, kernel):
    """Process the chat interaction asynchronously."""
    # Get the response from the chat completion service
    response = await chat_completion_service.get_chat_message_content(
        chat_history,
        settings,
        kernel=kernel
    )
    return response


def print_chat_history(chat_history):
    """Print the chat history in a formatted way."""
    print("\nChat History:")
    print("-" * 80)
    for message in chat_history.messages:
        print(f"Role: {message.role}")
        print(f"Content: {message.content}")
        if message.content == "":
            print(f"Details: {message.items}")
        print("-" * 80)


async def main():
    # Example catalog and schema names
    CATALOG = "puneetjain_uc"
    SCHEMA = "autogen_ucai"

    try:
        # Set up Unity Catalog client
        uc_client = setup_uc_client()

        # Create calculator function
        function_info = create_calculator_function(uc_client, CATALOG, SCHEMA)

        # Set up Semantic Kernel
        kernel, chat_completion_service, settings = setup_semantic_kernel()

        # Create toolkit instance
        toolkit = UCFunctionToolkit(
            function_names=[f"{CATALOG}.{SCHEMA}.add_numbers"],
            client=uc_client
        )

        # Register Unity Catalog functions with the kernel
        toolkit.register_with_kernel(kernel, plugin_name="calculator")

        # Create chat history
        chat_history = create_chat_history()

        # Process the chat interaction
        response = await process_chat(chat_history, chat_completion_service, settings, kernel)

        print("\nFinal Response:", response)

        # Print the detailed chat history
        print_chat_history(chat_history)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
