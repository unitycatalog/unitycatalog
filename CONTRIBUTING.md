# Contribution Guidelines

We happily welcome contributions to Unity Catalog UI. We use [GitHub Issues](https://github.com/unitycatalog/unitycatalog-ui/issues) to track community-reported issues and [GitHub Pull Requests](https://github.com/unitycatalog/unitycatalog-ui/pulls) for accepting changes. If your contribution is not straightforward, please first discuss the change you wish to make by creating a new issue before making the change.

## Governance

Unity Catalog UI is part of the Unity Catalog project, which is an independent open-source project and also a project of the [LF AI and Data Foundation](https://lfaidata.foundation/). The project under the Linux Foundation follows [open governance](https://github.com/opengovernance/opengovernance.dev), meaning there is no one company or individual in control of the project.

## Communication

- Before starting work on a major feature, please reach out to us via [GitHub](https://github.com/unitycatalog/unitycatalog-ui/issues), [Slack](https://unitycatalog.slack.com/), etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
- A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
- We will use the GitHub issue to discuss the feature and come to an agreement.
- This is to prevent your time being wasted, as well as ours.
- The GitHub review process for major features is also important so that organizations with commit access can come to an agreement on design.
- If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.
- Small patches and bug fixes don't need prior communication. If you have identified a bug and have ways to solve it, please create an [issue](https://github.com/unitycatalog/unitycatalog-ui/issues) or create a [pull request](https://github.com/unitycatalog/unitycatalog-ui/pulls).

## Code of Conduct

Please review our [Code of Conduct](https://github.com/unitycatalog/unitycatalog/blob/main/CODE_OF_CONDUCT.md) before participating. We expect all contributors to adhere to the guidelines outlined there.

## Reporting Issues

Before reporting an issue on the [issue tracker](https://github.com/unitycatalog/unitycatalog-ui/issues), please check that it has not already been reported by searching for some related keywords.

## Pull Requests

Try to do one pull request per change.

## How to Contribute

<details>
<summary> Please follow the steps from the nested dropdown to contribute to the project. </summary>

1. [Fork](https://github.com/unitycatalog/unitycatalog-ui/fork) the repository on GitHub.
2. Clone the forked repository to your local machine.

   ```sh
   git clone https://github.com/<your_github_username>/unitycatalog-ui.git
   ```

3. Get into the project directory

   ```sh
   cd unitycatalog-ui
   ```

4. Create your branch

   ```sh
   git checkout -b <your_branch_name>
   ```

5. Install the dependencies and start the development server

   ```sh
   yarn
   yarn start
   ```

6. Make your changes: Commit your changes with clear and concise commit messages.
7. Push your changes to your fork

   ```sh
   git push origin <your_branch_name>
   ```

8. Create a pull request to the `main` branch of the `unitycatalog/unitycatalog-ui` repository.
</details>

## Coding Style

We generally follow the [Apache Spark Scala Style Guide](https://spark.apache.org/contributing.html).

## Sign Your Work

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch.

```
Signed-off-by: Jane Smith <jane.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```

We are excited to work with the open-source communities in the many years to come to realize this vision. You can join the Unity Catalog open source community at unitycatalog.io and the [Unity Catalog Community Slack](https://go.unitycatalog.io/slack).
