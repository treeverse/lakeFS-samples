"""Deterministic curation brain.

``rules`` computes the one correct curation outcome for a corpus. ``validator``
independently verifies an agent's written outputs against that outcome and fails
closed. ``reports`` renders the human- and machine-readable reports. The model
may propose classifications, but only this code decides what is valid.
"""
