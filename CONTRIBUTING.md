# Contributing to Data Engineering for AI

This module is open to contributions from bootcamp cohorts, learners, and anyone who wants to improve it.

## How to Contribute

### Report Bugs and Issues

Found something broken or unclear?

1. Open an issue on GitHub.
2. Describe what went wrong.
3. Include the steps you took to reproduce it.
4. Include your environment: OS, Python version, whether you are using Docker.
5. Include any error messages you see.

### Suggest Improvements

Have an idea for better teaching, clearer explanation, or a missing concept?

1. Open an issue on GitHub.
2. Describe what you want to improve.
3. Explain why it matters.
4. Share your idea for how to do it, if you have one.

### Submit Code Changes

Improvements to the code, tests, or documentation are welcome.

1. Fork the repository.
2. Create a branch for your changes: `git checkout -b fix/description` or `git checkout -b feature/description`
3. Make your changes.
4. Test locally:
   ```bash
   docker compose up -d
   pytest tests/ -q
   ```
5. Push your branch and open a pull request.
6. In your pull request, describe what you changed and why.
7. Reference any related issues: "Fixes #123"

Tests do not need to pass if you are new to this. We can help.

## Code Style

This is a learning module, not a production system. Write code that is:

- Clear and easy to read
- Commented to explain why, not just what
- Consistent with the day's pattern: `lesson.py` at the top, work in `pipeline/`
- Type-hinted in config and pipeline code

We do not strictly enforce:

- Code formatting rules
- Complex test coverage
- Perfect commit messages

## Questions

Ask in an issue. Look at the day's walkthrough documentation. Run the tests to see what is expected.

## License

By contributing, you agree your work will be licensed under Apache License 2.0. See LICENSE for details.