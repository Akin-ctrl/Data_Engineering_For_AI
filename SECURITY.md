# Security Policy

This is a learning module, not a production system. However, we take security seriously.

## Reporting a Vulnerability

Do not open a public issue for security vulnerabilities. Instead:

1. Email the maintainer directly.
2. Include a clear description of the vulnerability.
3. Include steps to reproduce it, if possible.
4. Allow time for a response and fix before disclosing publicly.

Reports will be handled confidentially and promptly.

## What We Fix

We will prioritize fixes for vulnerabilities that:

- Affect the confidentiality, integrity, or availability of user data
- Compromise PostgreSQL credentials or environment variables
- Allow arbitrary code execution through the pipelines
- Break the isolation of student work environments

## What We Ask From You

- Give us reasonable time to fix issues before disclosure
- Do not exploit vulnerabilities for anything beyond confirming they exist
- Do not access other people's data or systems
- Do not disrupt the learning experience for others

## Scope

This policy applies to the Data Engineering for AI repository and its teaching code. It does not cover:

- External services like ArXiv, HuggingFace, or GitHub itself
- Student forks or modifications of this code
- Dependencies in requirements.txt (report those to their maintainers)

## Questions

If you are unsure whether something is a security issue, ask. Better to be cautious.