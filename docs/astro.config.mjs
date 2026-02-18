// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import { plausible } from "starlight-plausible";
import { githubStats } from "./plugins/starlight-github-stats";

// https://astro.build/config
export default defineConfig({
  site: "https://docs.unitycatalog.io/",
  image: {
    service: {
      entrypoint: "astro/assets/services/sharp",
    },
  },
  integrations: [
    starlight({
      plugins: [
        plausible({ domain: "unitycatalog.io", enableFeedback: true }),
        githubStats({ repo: "unitycatalog/unitycatalog" }),
      ],
      customCss: ["./src/styles/custom.css"],
      title: "Unity Catalog",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/unitycatalog/unitycatalog",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/unitycatalog/unitycatalog/docs",
      },
      lastUpdated: true,
      logo: {
        light: "./src/assets/unity-catalog-logo-light.svg",
        dark: "./src/assets/unity-catalog-logo-dark.svg",
        replacesTitle: true,
      },
      sidebar: [
        {
          label: "Home",
          link: "/",
        },
        {
          label: "Getting Started",
          items: [
            {
              label: "Quickstart",
              link: "/quickstart/",
            },
            {
              label: "Docker Compose",
              link: "/docker_compose/",
            },
          ],
        },
        {
          label: "Usage",
          items: [
            {
              label: "CLI",
              link: "/usage/cli/",
            },
            {
              label: "UI",
              link: "/usage/ui/",
            },
            {
              label: "Tables",
              items: [
                {
                  label: "Delta Lake",
                  link: "/usage/tables/deltalake/",
                },
                {
                  label: "Formats",
                  link: "/usage/tables/formats/",
                },
                {
                  label: "Uniform",
                  link: "/usage/tables/uniform/",
                },
              ],
            },
            {
              label: "Volumes",
              link: "/usage/volumes/",
            },
            {
              label: "Functions",
              link: "/usage/functions/",
            },
            {
              label: "Models",
              link: "/usage/models/",
            },
          ],
        },
        {
          label: "Server",
          items: [
            {
              label: "Configuration",
              link: "/server/configuration/",
            },
            {
              label: "Auth",
              link: "/server/auth/",
            },
            {
              label: "Users and Privileges",
              link: "/server/users-privileges/",
            },
          ],
        },
        {
          label: "Integrations",
          items: [
            {
              label: "Apache Spark",
              link: "/integrations/unity-catalog-spark/",
            },
            {
              label: "CelerData",
              link: "/integrations/unity-catalog-celerdata/",
            },
            {
              label: "Daft",
              link: "/integrations/unity-catalog-daft/",
            },
            {
              label: "DuckDB",
              link: "/integrations/unity-catalog-duckdb/",
            },
            {
              label: "Kuzu",
              link: "/integrations/unity-catalog-kuzu/",
            },
            {
              label: "PuppyGraph",
              link: "/integrations/unity-catalog-puppygraph/",
            },
            {
              label: "SpiceAI",
              link: "/integrations/unity-catalog-spiceai/",
            },
            {
              label: "Trino",
              link: "/integrations/unity-catalog-trino/",
            },
            {
              label: "XTable",
              link: "/integrations/unity-catalog-xtable/",
            },
          ],
        },
        {
          label: "AI",
          items: [
            {
              label: "Quickstart",
              link: "/ai/quickstart/",
            },
            {
              label: "Usage",
              link: "/ai/usage/",
            },
            {
              label: "AI Client",
              link: "/ai/client/",
            },
            {
              label: "Integrations",
              items: [
                {
                  label: "LangChain",
                  link: "/ai/integrations/langchain/",
                },
                {
                  label: "LlamaIndex",
                  link: "/ai/integrations/llamaindex/",
                },
                {
                  label: "OpenAI",
                  link: "/ai/integrations/openai/",
                },
                {
                  label: "Anthropic",
                  link: "/ai/integrations/anthropic/",
                },
                {
                  label: "CrewAI",
                  link: "/ai/integrations/crewai/",
                },
                {
                  label: "AutoGen",
                  link: "/ai/integrations/autogen/",
                },
                {
                  label: "LiteLLM",
                  link: "/ai/integrations/litellm/",
                },
                {
                  label: "Gemini",
                  link: "/ai/integrations/gemini/",
                },
              ],
            },
          ],
        },
      ],
    }),
  ],
});
