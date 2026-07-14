# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in AetherFlow, please **do not open a public
issue**. Instead, report it privately by email to **mash180sx@gmail.com** with:

- a description of the issue and its impact,
- steps to reproduce (a minimal example if possible),
- the affected version / commit.

You can expect an initial acknowledgement within a few days. We will work with you to
understand and address the issue before any public disclosure, and will credit you in the
release notes unless you prefer to remain anonymous.

## Supported Versions

AetherFlow is pre-1.0 (`0.x`). Security fixes are applied to the latest released `0.x`
version. Pin an exact version if you need stability while we iterate.

## Scope

AetherFlow is a concurrency runtime. Reports most relevant to us include: memory-safety
issues in the `unsafe` code (SPSC/MPSC queues, the ask reply-cell), soundness holes that
break the compile-time isolation guarantees, and denial-of-service vectors in the scheduler
or mailboxes. Application-level misuse of the API is generally out of scope.
