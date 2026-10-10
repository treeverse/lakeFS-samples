"""AgentCore Policy (Cedar) model, rendering, and tests.

``authz`` is the single source of truth for which curated tools the agent may
call and which are forbidden. It renders the concrete Cedar policy text the
deploy script pushes to the AgentCore policy engine, so the model the tests
assert against and the policy actually enforced are derived from one place.
"""
