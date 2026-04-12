# Security Policy

## Supported Versions

Only the current `main` branch is actively maintained.
Older branches or tags are not guaranteed to receive security fixes.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues by emailing the repository maintainer
directly (see repository owner contact on GitHub).  Include:

- A description of the vulnerability and its impact
- Steps to reproduce or a proof-of-concept (if safe to share)
- The affected component(s) and version/commit

You will receive an acknowledgement within 72 hours.
Confirmed vulnerabilities will be patched and disclosed responsibly.

## Scope

This repository contains automated trading software that interacts with
the Betfair exchange API.  Particularly sensitive areas include:

- Credential storage (`database.py`, `core/secret_cipher.py`)
- API session management (`betfair_client.py`, `services/betfair_service.py`)
- Order execution and safety layers (`core/runtime_controller.py`, `core/safety_layer.py`)

## Out of Scope

- Issues in third-party dependencies (report directly to the dependency maintainer)
- Theoretical vulnerabilities without a realistic attack path
