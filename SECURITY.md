# Security Policy

IronCache is in its research and specification phase and has no shipping code
yet. There is therefore no released artifact to report a vulnerability against.
This policy is published early so the reporting path exists from day one.

## Reporting a vulnerability

Please report suspected security issues privately. Do not open a public issue
for a vulnerability.

- Preferred: use GitHub's private vulnerability reporting on this repository
  (the "Report a vulnerability" button under the Security tab).
- Alternatively, email the maintainers at security@ironcache.dev.

You will receive an acknowledgement, and we will work with you on a coordinated
disclosure timeline. We will credit reporters who wish to be credited.

## Scope

Once IronCache ships code, the threat model (an authenticated, network-facing
cache that speaks the Redis wire protocol, with optional persistence and
clustering) will be documented in `docs/THREAT_MODEL.md` and this policy will be
expanded to cover supported versions.
