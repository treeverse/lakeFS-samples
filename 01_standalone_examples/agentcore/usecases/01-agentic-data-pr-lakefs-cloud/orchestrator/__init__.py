"""Shared business logic used by BOTH the CLI scripts and the Streamlit UI.

Nothing in ``scripts/`` or ``ui/`` reimplements demo logic; they call these
functions and render the results. This keeps the CLI and UI behaviourally
identical, as required.
"""
