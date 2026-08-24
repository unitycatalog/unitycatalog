# Unity Catalog AI

Welcome to **Unity Catalog AI**, your comprehensive solution for integrating powerful AI functionalities with Unity Catalog. Whether you're a GenAI developer looking to enhance your applications or a Data Engineer seeking robust governance and access control, Unity Catalog AI bridges the gap between advanced AI tools and enterprise-grade data management.

---

## What is Unity Catalog AI?

**Unity Catalog AI** is a core library designed to seamlessly integrate Unity Catalog with various Generative AI (GenAI) frameworks and tools. It allows you to define, manage, and execute functions within Unity Catalog and utilize them as tools in your GenAI applications. By leveraging Unity Catalog's robust data governance and management capabilities, Unity Catalog AI ensures that your AI tools are secure, well-organized, and easily accessible.

---

## Key Concepts

### GenAI Tools

**GenAI Tools** are external functions or services that Generative AI models can invoke to perform specific tasks. These tools extend the capabilities of AI models by allowing them to interact with external systems, perform computations, access databases, and more. In the context of Unity Catalog AI, GenAI Tools are UC functions that have been integrated into AI workflows, enabling seamless interaction between AI models and your managed functions.

### UC Functions

**UC Functions** are functions defined and stored within Unity Catalog. They encapsulate specific business logic or operations that can be executed on demand. By storing these functions in Unity Catalog, you benefit from centralized management, versioning, and access control. UC Functions can be invoked as tools within GenAI applications, allowing AI models to utilize these predefined operations securely and efficiently.

---

## Why Unity Catalog AI?

### Simplified Agent Authoring

Creating and managing agents in GenAI applications can be complex, especially when dealing with multiple tools and ensuring they operate seamlessly. Unity Catalog AI simplifies Agent Authoring by:

- **Handling Access Control**: Automatically manages permissions, ensuring that only authorized agents can invoke specific UC Functions.
- **Parsing and Execution**: Takes care of parsing function inputs and executing them correctly, reducing the overhead on developers.
- **Native Integrations**: Provides per-library integrations, allowing developers to use UC Functions as tools without extensive setup or configuration.

### Enhanced Access Control

Security is paramount when dealing with data and AI functionalities. Unity Catalog AI leverages Unity Catalog's robust access control mechanisms to ensure that:

- **Function Access is Secured**: Only authorized users and agents can access and execute UC Functions.
- **Granular Permissions**: Define precise permissions at the catalog, schema, and function levels.
- **Auditability**: Track and audit function usage for compliance and monitoring purposes.

### Seamless Integrations

Unity Catalog AI offers native integrations with a variety of popular GenAI frameworks, making it easy to incorporate UC Functions into your existing workflows. Supported integrations include:

- [LangChain](./integrations/langchain.md)
- [LlamaIndex](./integrations/llamaindex.md)
- [OpenAI](./integrations/openai.md)
- [Anthropic](./integrations/anthropic.md)
- [CrewAI](./integrations/crewai.md)
- [AutoGen](./integrations/autogen.md)
- [LiteLLM](./integrations/litellm.md)
- [Gemini](./integrations/gemini.md)

Each integration is designed to provide a smooth and intuitive experience, allowing you to focus on building intelligent applications without worrying about the underlying infrastructure.

---

## Getting Started

Embark on your journey with Unity Catalog AI by following our [Quickstart Guide](./quickstart.md). This guide will walk you through the essential steps of installing the library, setting up your environment, and creating and executing UC Functions. Whether you're using an open-source UC server or a Databricks-managed UC, our quickstart ensures you can get up and running swiftly.

---

## Integrations

Unity Catalog AI integrates with a range of GenAI frameworks to enhance your AI applications. Explore our [Integrations Page](./integrations/index.md) to discover detailed guides and example notebooks for each supported framework.

---

## Who Should Use Unity Catalog AI?

Unity Catalog AI is designed for:

- **GenAI Developers**: Enhance your AI models with robust, secure, and manageable tools.
- **Data Engineers**: Leverage Unity Catalog's data governance capabilities to manage and execute AI functions seamlessly.
- **AI Teams**: Collaborate efficiently by centralizing function definitions and access controls.
- **Enterprise Applications**: Integrate AI functionalities into large-scale applications with ease, ensuring security and compliance.

---

## Learn More

- **[Support](mailto:support@unitycatalog.com)**: Reach out to our support team for assistance.
- **[GitHub Repository](https://github.com/unitycatalog/unitycatalog)**: Explore the source code, report issues, and contribute to the project.

---

## Get Involved

Join our community to stay updated on the latest developments, share your experiences, and collaborate with other users:

- **[Community Forum](https://community.unitycatalog.com/)**

---

*Empower your AI applications with the structured and secure capabilities of Unity Catalog AI.*
