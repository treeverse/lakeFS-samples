"""Server-side Gateway pieces.

``lakefs_client`` is a thin, dependency-light lakeFS Cloud REST client used both
by the Lambda (behind the Gateway) and by the local approval scripts.
``validation`` holds the input-validation guards every capability runs.
``handler`` is the AWS Lambda entrypoint implementing the curated capabilities.
"""
