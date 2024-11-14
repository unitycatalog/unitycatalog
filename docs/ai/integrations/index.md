# Unity Catalog AI Integrations

## LangChain

**LangChain** is a powerful framework for developing applications powered by language models. The Unity Catalog AI integration with [LangChain](https://www.langchain.com/) and [LangGraph](https://www.langchain.com/langgraph) allows you to seamlessly incorporate functions defined in Unity Catalog as tools within your LangChain and LangGraph Agentic workflows, enhancing the capabilities and flexibility of your language model applications.

Installation:

```sh
pip install unitycatalog-langchain
```

To learn more about this integration:

[Guide](langchain.md): Step-by-step instructions to integrate Unity Catalog AI with LangChain.
[Example Notebook](https://github.com/unitycatalog/unitycatalog/blob/main/ai/integrations/langchain/ucai-langchain_sample.ipynb): A Jupyter notebook demonstrating practical usage of the integration.

## LlamaIndex

[LlamaIndex](https://www.llamaindex.ai/) is a data framework for LLM applications. The Unity Catalog AI integration with LlamaIndex enables you to utilize UC functions as part of your data indexing and querying workflows, providing a robust and scalable solution for managing large datasets.

Installation:

```sh
pip install unitycatalog-llamaindex
```

To learn more about this integration:

[Guide](llamaindex.md): Detailed guide on integrating Unity Catalog AI with the LlamaIndex framework.
[Example Notebook](https://github.com/unitycatalog/unitycatalog/blob/main/ai/integrations/llama_index/llama_index_sample.ipynb)

## OpenAI

The [OpenAI](https://openai.com/) integration with Unity Catalog AI allows you to harness the capabilities of OpenAI's language models while managing and utilizing UC functions as tools within your OpenAI-powered applications. This integration enhances your ability to build intelligent and responsive applications with ease.

Installation:

```sh
pip install unitycatalog-openai
```

To learn more about this integration:

[Guide](openai.md): Detailed guide on integrating Unity Catalog AI with OpenAI's sophisticated GPT-based systems.
[Example Notebook](https://github.com/unitycatalog/unitycatalog/blob/main/ai/integrations/openai/ucai-openai_sample.ipynb)

## Anthropic

[Anthropic](https://www.anthropic.com/) offers cutting-edge language models designed with a focus on safety and reliability. The Unity Catalog AI integration with Anthropic enables you to utilize UC functions as tools within Anthropic's language model calls, enhancing the functionality and control of your AI applications.

Installation:

```sh
pip install unitycatalog-anthropic
```

To learn more about this integration:

[Anthropic Guide](anthropic.md): Detailed guide on integrating Unity Catalog AI with Anthropic's powerful Claude LLMs.
[Example Notebook](https://github.com/unitycatalog/unitycatalog/blob/main/ai/integrations/anthropic/anthropic_sample.ipynb)

## CrewAI

[CrewAI](https://www.crewai.com/) is a collaborative AI framework that allows multiple agents to work together to accomplish complex tasks. The Unity Catalog AI integration with CrewAI enables you to define CrewAI Tools directly within your agent definitions, leveraging UC functions to enhance agent capabilities and streamline workflows.

Installation:

```sh
pip install unitycatalog-crewai
```

To learn more about this integration:

[Guide](crewai.md): Detailed guide on integrating Unity Catalog AI with CrewAI Agents.

## AutoGen

[AutoGen](https://microsoft.github.io/autogen/0.2/) is a framework designed to facilitate the creation of multi-agent workflows. The Unity Catalog AI integration with AutoGen allows for the direct application of UC functions to distinct agents within AutoGen applications or via universal application mapping, enhancing the capabilities of multi-turn agentic workflows.

Installation:

```sh
pip install unitycatalog-autogen
```

To learn more about using UC functions as tools within AutoGen's framework:

[Guide](autogen.md): Detailed guide on integrating Unity Catalog AI with AutoGen's multi-agent framework.
