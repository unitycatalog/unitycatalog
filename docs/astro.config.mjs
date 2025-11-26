// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import netlify from "@astrojs/netlify";

// https://astro.build/config
export default defineConfig({
  site: "https://docs.unitycatalog.io/",
  image: {
    service: {
      entrypoint: "astro/assets/services/sharp",
    },
  },
  adapter: netlify(),
  integrations: [
    starlight({
      customCss: ["./src/styles/custom.css"],
      title: "Unity Catalog",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/unitycatalog/unitycatalog",
        },
      ],
      head: [
        {
          tag: "script",
          content: `
            document.addEventListener('DOMContentLoaded', async function() {
              console.log('GitHub stats script loaded');
              
              const socialContainer = document.querySelector('.sl-flex.social-icons') || 
                                    document.querySelector('[class*="social"]') ||
                                    document.querySelector('.sl-header__actions');
              
              if (socialContainer) {
                console.log('Social container found, fetching GitHub data');
                
                try {
                  // Fetch repository data from GitHub API
                  const response = await fetch('https://api.github.com/repos/unitycatalog/unitycatalog');
                  const data = await response.json();
                  
                  // Format numbers
                  const formatNumber = (num) => {
                    if (num >= 1000) {
                      return (num / 1000).toFixed(1) + 'k';
                    }
                    return num.toString();
                  };
                  
                  // Get latest release version
                  let latestVersion = 'v0.3.0'; // fallback
                  try {
                    const releaseResponse = await fetch('https://api.github.com/repos/unitycatalog/unitycatalog/releases/latest');
                    const releaseData = await releaseResponse.json();
                    latestVersion = releaseData.tag_name || latestVersion;
                  } catch (e) {
                    console.log('Could not fetch latest release, using fallback version');
                  }
                  
                  const githubStats = document.createElement('div');
                  githubStats.innerHTML = \`
                    <div class="github-stats">
                      <a href="https://github.com/unitycatalog/unitycatalog" title="Go to repository" class="github-link">
                        <div class="github-info">
                          <ul class="github-facts">
                            <li class="github-fact github-fact--version">\${latestVersion}</li>
                            <li class="github-fact github-fact--stars">\${formatNumber(data.stargazers_count)}</li>
                            <li class="github-fact github-fact--forks">\${formatNumber(data.forks_count)}</li>
                          </ul>
                        </div>
                      </a>
                    </div>
                  \`;
                  githubStats.className = 'github-stats-container';
                  socialContainer.appendChild(githubStats);
                  console.log('GitHub stats added with live data');
                  
                } catch (error) {
                  console.log('Failed to fetch GitHub data, using fallback');
                  // Fallback to static data
                  const githubStats = document.createElement('div');
                  githubStats.innerHTML = \`
                    <div class="github-stats">
                      <a href="https://github.com/unitycatalog/unitycatalog" title="Go to repository" class="github-link">
                        <div class="github-info">
                          <ul class="github-facts">
                            <li class="github-fact github-fact--version">v0.3.0</li>
                            <li class="github-fact github-fact--stars">3.1k</li>
                            <li class="github-fact github-fact--forks">523</li>
                          </ul>
                        </div>
                      </a>
                    </div>
                  \`;
                  githubStats.className = 'github-stats-container';
                  socialContainer.appendChild(githubStats);
                }
              } else {
                console.log('Social container not found');
              }
            });
          `,
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
