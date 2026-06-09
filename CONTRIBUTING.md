# Contributing to KCelery

We welcome contributions to KCelery! Here are some guidelines to help you get started.

## How to Contribute

1.  **Fork the repository**: Start by forking the KCelery repository to your GitHub account.
2.  **Clone your fork**: Clone your forked repository to your local machine.
    ```bash
    git clone https://github.com/vick-ram/kcelery.git
    cd kcelery
    ```
3.  **Create a new branch**: Create a new branch for your feature or bug fix.
    ```bash
    git checkout -b feature/your-feature-name
    # or
    git checkout -b bugfix/your-bug-fix-name
    ```
4.  **Make your changes**: Implement your feature or fix the bug. Ensure your code adheres to the project's coding style and conventions.
5.  **Write tests**: Add appropriate unit or integration tests for your changes.
6.  **Run tests**: Make sure all existing tests pass, along with your new tests.
    ```bash
    ./gradlew test
    ```
7.  **Commit your changes**: Write clear and concise commit messages.
    ```bash
    git commit -m "feat: Add new feature X"
    # or
    git commit -m "fix: Resolve bug Y"
    ```
8.  **Push to your fork**: Push your changes to your forked repository.
    ```bash
    git push origin feature/your-feature-name
    ```
9.  **Create a Pull Request**: Open a pull request from your branch to the `main` branch of the original KCelery repository. Provide a detailed description of your changes.

## Code Style

*   Follow Kotlin coding conventions.
*   Use `ktlint` for code formatting.

## Reporting Bugs

If you find a bug, please open an issue on GitHub with the following information:

*   A clear and concise description of the bug.
*   Steps to reproduce the behavior.
*   Expected behavior.
*   Screenshots or error messages (if applicable).
*   Your environment details (KCelery version, Kotlin version, Redis version, JVM version).

## Feature Requests

We'd love to hear your ideas! Please open an issue on GitHub to suggest new features or improvements.

## Development Setup

Refer to the `README.md` file for instructions on setting up your development environment.

Thank you for contributing!
