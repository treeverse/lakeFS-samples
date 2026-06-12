# Custom E2B template for the receipts-ledger agent.
#
# Bakes the slow, stable pieces so each run starts fast:
#   - FUSE userspace (so `everest mount --protocol fuse` works)
#   - the everest (lakeFS Mount) Linux binary
#   - the agent's python deps
#
# The agent package itself is NOT baked — the orchestrator uploads it fresh each run,
# and /dev/fuse permissions are set per-sandbox at runtime by provision().
#
# Build context = this directory. Before building, copy the gitignored Linux binary here:
#   cp ../build/bin/everest ./everest
# Then:  e2b template build --name mount-receipts
FROM e2bdev/code-interpreter:latest

# System packages need root (the base image's default build user is non-root).
USER root

# FUSE userspace
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 \
 && rm -rf /var/lib/apt/lists/* \
 && echo user_allow_other >> /etc/fuse.conf

# everest binary (lakeFS Mount). Provided in the build context (see header).
COPY everest /usr/local/bin/everest
RUN chmod +x /usr/local/bin/everest

# Agent python dependencies — installed system-wide (as root) so the agent, which runs
# as root (so the FUSE mount is visible to E2B's root-owned envd / Filesystem tab), can
# import them. Installing as the 'user' would put them in ~/.local, unreachable by root.
RUN pip install --no-cache-dir openai pillow pymupdf python-dateutil
