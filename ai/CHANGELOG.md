# Changelog for Unity Catalog AI releases

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
