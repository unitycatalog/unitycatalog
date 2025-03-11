# Changelog for Unity Catalog AI releases

## Unity Catalog AI 0.3.0

The 0.3.0 release of Unity Catalog AI includes several usability improvements, a safer function execution mode, features that expand the functionality of versatile function usage and enhancements to aid in more flexible composition of complex functions.

### Major Changes

- 🛡️ **Enhanced Security**: Functions now execute in a safer Process pool by default (using `'sandbox'` `execution_mode`), replacing the original main process implementation. This allows for timeout restrictions, CPU resource limitations, and blocks modules that access system resources. The legacy mode remains available via the `'local'` `execution_mode` parameter within the client constructor.

- 🐞 **Developer-Friendly Debugging**: A new development-only execution mode for `DatabricksFunctionClient` enables local subprocess execution with runtime protection and timeout controls. While not for production use, this simplifies debugging and rapid iteration without requiring serverless cluster execution. Configure by changing the `execution_mode` from `'serverless'` to `'local'`.

- 🔌 **Streamlined Integration in Databricks**: When using integration packages (e.g., `unitycatalog-openai`) with a `UCFunctionToolkit` instance, you no longer need to explicitly declare a `DatabricksFunctionClient` within the toolkit interface when a serverless connection is available and the ability to create a `WorkspaceClient` is met. A default connection will be established using standard credentials unless custom configuration is required.

- 🔄 **Improved Connection Reliability**: Fixed credential refresh issues in the `DatabricksFunctionClient`, including the 24-hour cluster idle time error. Connections to inactive serverless compute instances now robustly reacquire authentication credentials.

- 🧩 **Python Callable Access**: New `get_function_as_callable` APIs allow retrieval of UC-registered Python functions as direct Python callables, significantly improving developer productivity when testing, debugging, and validating complex functions.

- 🔍 **Function Source Inspection**: Added `get_function_source` APIs to retrieve UC Python function definitions as strings, enabling easier inspection, debugging, modification, and reuse of existing functions.

- 🤫 **Warning Suppression**: The persistent Pydantic V2 warning from LangChain API calls has been silenced.

### Change log

- Enable the ability to define a `UCFunctionToolkit` instance when a Databricks compute environment is available that will use the default serverless connection configuration if a client instance is not supplied to the constructor by @BenWilson2 in #938
- Remove the included and outdated external `asyncio` package from the build files which causes issues with Python 3.13 by @BenWilson2 in #937
- Address invoker access rights in the DatabricksFunctionClient by @aravind-segu in #933
- Bridge the Workspace client configuration to the Databricks Connect interface by @aravind-segu in #930
- Add support for safer local function execution in Unity Catalog and add a development-mode local execution mode for Databricks clients by @BenWilson2 in #925
- Silence the LangChain pydantic version warning by @BenWilson2 in #927
- Fix an issue where a connection to a serverless compute instance would not properly refresh client credentials and would not recover from a terminated instance by @BenWilson2 in #918
- Remove the explicit version pinning for the databricks-connect dependency by @BenWilson2 in #928
- Fix an issue in the toolkit creation example where a client definition was missing by @avriiil in #915
- Add functionality for callable retrieval as a string definition from a registered UC Python function by @BenWilson2 in #912
- Fix the toolkit instantiation example for LlamaIndex within the documentation usage guide by @BenWilson2 in #906
- Fix the documentation guide sidebar to properly display all package integrations by @BenWilson2 in #900
- Update links in the documentation to point to the correct notebooks within the repository by @avriiil in #894

## Unity Catalog AI 0.2.0

The 0.2.0 release of Unity Catalog AI brings some exciting new functionality for using Unity Catalog for GenAI function execution.

### Major Changes

- [unitycatalog-autogen](https://pypi.org/project/unitycatalog-autogen) has been overhauled to support the new `0.4.x` overhaul of [AutoGen](https://www.microsoft.com/en-us/research/project/autogen/). Support for earlier versions of the `autogen` or `pyautogen` packages has been dropped. To read more about what this reimagining of the library is all about and to learn how to migrate to the new APIs, see [this post](https://www.microsoft.com/en-us/research/blog/autogen-v0-4-reimagining-the-foundation-of-agentic-ai-for-scale-extensibility-and-robustness/).
- [unitycatalog-gemini](https://pypi.org/project/unitycatalog-gemini) has been released. You can now use Unity Catalog functions as tools when interfacing with Gemini models that support tool calling.
- [unitycatalog-litellm](https://pypi.org/project/unitycatalog-litellm) has been released. You can now interface with the universal common interface of LiteLLM to pass tool usage definitions to integrated providers in LiteLLM that support tool calling.
- New APIs have been added for function wrapping. This change eliminates the need for making multiple copies of utility functions that need to be embedded in your Unity Catalog functions, greatly simplifying the devloop for authoring. See the documentation for examples on how to use these new APIs!
- The Databricks Unity Catalog Client now only supports serverless endpoints using dbconnect for all CRUD and execution APIs. This change aims to simpilfy the usage of the client APIs and to reduce latency in cold start scenarios.
- Support for `requirements`, `environment_version` and `Variant` type have been added to the Databricks client. Ensure that the runtime that you are using supports these features to leverage the power of dependency management and JSON-like inputs and outputs to your functions.

### Change log

- Fix an issue with default parameter value handling during function execution and permit handling of default `NULL` and boolean SQL values by @BenWilson2 in #896
- Add documentation for `unitycatalog-gemini` and `unitycatalog-litellm` by @BenWilson2 in #881 and #888
- Add support for declaring both `dependencies` and `environment_version` when creating functions in Databricks, allowing for additional PyPI packages to be included when executing your functions by @BenWilson2 in #862
- Add support for function wrapping in new client APIs by @BenWilson2 in #885
- Adjust the frequency of warnings associated with endpoint execution of functions by @BenWilson2 in #880
- Documentation cleanup by @BenWilson2 in #886
- Add support for `Variant` types in Databricks Unity Catalog Client by @BenWilson2 in #889
- Introduce the `unitycatalog-litellm` integration that permits tool usage with supported LLMs when using the LiteLLM package by @micheal-berk in #699
- Remove support for non-serverless workspace endpoints within the Databricks Client to simplify API usage by @BenWilson2 in #873
- Add support for retriever-based functions having a native integration with MLflow tracing by @annzhang-db in #861
- Correct issues with the `anthropic` test suites to ensure that API signatures with citation references are covered by the integration by @BenWilson2 in #865
- Introduce the `unitycatalog-gemini` integration that permits tool usage with Google Gemini endpoints by @puneet-jain159 in #808
- Add a cache invalidator to the function cache in the Unity Catalog Client to support a more seamless devloop by @BenWilson2 in #853
- Refactor the `AutoGen` integration in `unitycatalog-autogen` to work with versions 0.4.0 and later by @BenWilson2 in #852 and #875
- Fix integration tests for retriever tracing integration by @serena-ruan in #854 and #860
- Update `AutoGen` documentation and build files to target the official Microsoft Research supported package by @BenWilson2 in #813 and #843
- Update the `CrewAI` integration to support new API arguments for tool calling by @BenWilson2 in #825
- Fix an issue with the `list_functions` API to allow returning function metadata for users that do not have access to the underlying function definition by @serena-ruan in #804

## Unity Catalog AI 0.1.0

We are excited to announce the initial release of Unity Catalog AI! This initial release covers the following features and packages.

Packages included in this release:

- [unitycatalog-ai](https://pypi.org/project/unitycatalog-ai/) - the core client package for function CRUD operations and execution of functions as GenAI tools.
- [unitycatalog-langchain](https://pypi.org/project/unitycatalog-langchain/) - Integration package that allows for using UC functions as tools within [LangChain](https://github.com/langchain-ai/langchain) and [LangGraph](https://github.com/langchain-ai/langgraph).
- [unitycatalog-llamaindex](https://pypi.org/project/unitycatalog-llamaindex/) - Integration package for using UC functions as Agent tools within [LlamaIndex](https://github.com/run-llama/llama_index).
- [unitycatalog-openai](https://pypi.org/project/unitycatalog-openai/) - Integration package to utilize tool calling functionality with the [OpenAI SDK](https://github.com/openai/openai-python).
- [unitycatalog-anthropic](https://pypi.org/project/unitycatalog-anthropic/) - Integration package to allow Claude models to use UC functions as tools with the [Anthropic SDK](https://github.com/anthropics/anthropic-sdk-python).
- [unitycatalog-crewai](https://pypi.org/project/unitycatalog-crewai/) - Integration package for Crew usage of UC functions as tools within [CrewAI](https://github.com/crewAIInc/crewAI).
- [unitycatalog-autogen](https://pypi.org/project/unitycatalog-autogen/) - Integration package for the use of UC functions within [AutoGen](https://github.com/microsoft/autogen).

### Unity Catalog Functions Client

The `unitycatalog-ai` package contains the interface for connecting to either a Unity Catalog server or a managed Databricks Unity Catalog service.
With this library, you can:

- Create Catalogs and Schemas
- List and Delete Unity Catalog Functions
- Create Unity Catalog functions from Python Callable definitions
- Execute a Unity Catalog Function

The client that is created within this package is used within the integration packages listed below to retrieve and execute tool calls from GenAI applications.

### Integration Packages

The initial release of the integration packages allows for seamless definition of GenAI tools within agent code for GenAI applications. The unified `UCFunctionToolkit` interface is identical among all of them, providing a unified tool definition interface that will interact with each of the integrated libraries without any complex tool handling code required.

### Change log

- Adjust logic for `list_functions` to support metadata-only queries by @serena-ruan in #804
- Refine tutorials and README files for initial release by @BenWilson2 in #797
- Add integration tests for each library integration to utilize a running UC server by @BenWilson2 in #790, #789, #788
- Add catalog and schema create utility methods to the functions clients by @BenWilson2 in #765
- Update the `unitycatalog-ai` README for connection guidance using the official `unitycatalog-client` SDK by @BenWilson2 in #775
- Increase the default byte limit for returned values from function calls to support large return payloads by @serena-ruan in #760
- Add support for the Unity Catalog client to use the `unitycatalog-client` SDK by @BenWilson2 in #748
- Add end-to-end Jupyter Notebook tutorials by @BenWilson2 in #763
- Add LlamaIndex integration tests by @BenWilson2 in #741
- Update pyproject.toml configurations to support additional package installation options by @BenWilson2 in #737
- Add Unity Catalog AI documentation to the Unity Catalog docs by @BenWilson2 in #625
- Fix an issue with the CrewAI integration suite by @serena-ruan in #746
- Add warnings to insufficiently annotated function creation when using SQL body statements to create functions by @BenWilson2 in #726
- Add end-to-end examples of integrations when using Databricks managed Unity Catalog by @serena-ruan in #691
- Address Spark Session issues when running function execution on Databricks by @serena-ruan in #724
- Fix a bug with function information validation by @serena-ruan in #700
- Add usage warnings for externally created functions that do not have appropriate function comments for GenAI usage by @BenWilson2 in #690
- Fix a bug with parsing string inputs when executing a function on Databricks serverless warehouses by @serena-ruan in #689
- Add support for CrewAI as a library integration by @michael-berk in #633
- Add support for AutoGen as a library integration by @puneet-jain159 in #644
- Adjust the sanitization logic of input parameters for function execution to only apply locally by @BenWilson2 in #674
- Fix flaky integration suites by @BenWilson2 in #664
- Add escape sequences for parameter inputs that are code blocks (GenAI-generated code execution) by @BenWilson2 in #660
- Add function names to tool definitions by @serena-ruan in #658
- Relocate integration suites to ai directory by @BenWilson2 in #663
- Add support for UC function execution with parameter-less function calls by @serena-ruan in #661
- Add tutorials for Anthropic and LlamaIndex by @BenWilson2 in #653
- Refactor to migrate to the ai directory by @BenWilson2 in #652
- Convert unitycatalog-ai and all integrations to be namespace packages by @BenWilson2 in #643
- Add notebook examples for OpenAI and LangChain integrations by @serena-ruan in #627
- Add client retry logic for Databricks function Client by @BenWilson2 in #626
- Add integration suites for LlamaIndex by @BenWilson2 in #615
- Add support for Anthropic as an integration package by @BenWilson2 in #614
- Enhance docstring parsing for Python callable metadata parsing by @BenWilson2 in #612
- Add support for OpenAI as an integration package by @serena-ruan in #611
- Add support for LangChain as an integration package by @serena-ruan in #610
- Add function utilies, callable utilities, and refactor for CI setup by @serena-ruan, @BenWilson2 in #605, #606, #609
- Add support for defining a Python callable to create a UC function by @BenWilson2 in #577
- Add support for executing functions on Databricks by @serena-ruan in #578
- Add client tests for Databricks by @serena-ruan in #538
- Add client utilities for Databricks by @serena-ruan in #537
- Implement core client library for Databricks by @serena-ruan in #536
