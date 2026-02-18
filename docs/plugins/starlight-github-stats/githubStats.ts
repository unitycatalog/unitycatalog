import type { AstroIntegration } from "astro";
import type { StarlightPlugin } from "@astrojs/starlight/types";

interface IntegrationOptions {
  /* Github repo to load stats for */
  repo: string;
}

type PluginOptions = IntegrationOptions;

const githubStatsIntegration = (
  options: IntegrationOptions,
): AstroIntegration => {
  const { repo } = options;

  return {
    name: "starlight-github-stats-integration",
    hooks: {
      "astro:config:setup": ({ injectScript }) => {
        injectScript(
          "page",
          `
          document.addEventListener('DOMContentLoaded', async function() {
            console.log('GitHub stats script loaded');
            
            const socialContainer = document.querySelector('.sl-flex.social-icons') || 
                                  document.querySelector('[class*="social"]') ||
                                  document.querySelector('.sl-header__actions');
            
            if (socialContainer) {
              console.log('Social container found, fetching GitHub data');
              
              try {
                // Fetch repository data from GitHub API
                const response = await fetch('https://api.github.com/repos/${repo}');
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
                  const releaseResponse = await fetch('https://api.github.com/repos/${repo}/releases/latest');
                  const releaseData = await releaseResponse.json();
                  latestVersion = releaseData.tag_name || latestVersion;
                } catch (e) {
                  console.log('Could not fetch latest release, using fallback version');
                }
                
                const githubStats = document.createElement('div');
                githubStats.innerHTML = \`
                  <div class="github-stats">
                    <a href="https://github.com/${repo}" title="Go to repository" class="github-link">
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
                    <a href="https://github.com/${repo}" title="Go to repository" class="github-link">
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
        );
      },
    },
  };
};

export const githubStats = (options: PluginOptions): StarlightPlugin => {
  return {
    name: "starlight-github-stats",
    hooks: {
      "config:setup": ({ config, updateConfig, addIntegration }) => {
        updateConfig({
          customCss: [
            ...(Array.isArray(config?.customCss) ? config.customCss : []),
            "./plugins/starlight-github-stats/githubStats.css",
          ],
        });
        addIntegration(githubStatsIntegration(options));
      },
    },
  };
};
