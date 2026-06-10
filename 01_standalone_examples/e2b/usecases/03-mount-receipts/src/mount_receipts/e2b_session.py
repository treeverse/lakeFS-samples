"""E2B sandbox lifecycle for lakeFS Mount: provision → mount → run → commit → umount.

The agent's work happens *inside* the sandbox on the mounted branch, so this module
runs everything via ``sandbox.commands.run`` over an ``everest`` FUSE mount. Credentials
reach the sandbox only as env vars (set at ``Sandbox.create``), never as CLI flags.

When a prebaked template is used (``cfg.e2b_template``), ``provision`` skips the apt/pip/
binary installs — the template already contains everest, fuse, the deps, and the package.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass

_HERE = pathlib.Path(__file__).resolve()
PKG_DIR = _HERE.parent                                   # src/mount_receipts
USECASE_DIR = _HERE.parents[2]                           # usecases/03-mount-receipts
EVEREST_BIN = USECASE_DIR / "build" / "bin" / "everest"  # gitignored Linux binary

# Only the modules the in-sandbox agent needs (agent_runner + its imports).
SANDBOX_MODULES = ["__init__.py", "validation.py", "extraction.py", "agent_runner.py"]

MOUNT_DIR = "/home/user/mnt"
SANDBOX_PKG_ROOT = "/home/user"                          # so `python -m mount_receipts...` resolves
PIP_DEPS = "openai pillow pymupdf python-dateutil"


@dataclass
class CmdResult:
    cmd: str
    exit_code: int
    stdout: str
    stderr: str


try:  # the e2b SDK raises this on any non-zero command exit
    from e2b.sandbox.commands.command_handle import CommandExitException
except Exception:  # pragma: no cover - import-path safety across SDK versions
    CommandExitException = None


def run(sbx, cmd: str, *, timeout: int = 300, check: bool = True) -> CmdResult:
    """Run a command. ``sbx.commands.run`` raises on non-zero exit, so catch that and
    surface a CmdResult; only re-raise (as RuntimeError) when ``check`` is set."""
    try:
        r = sbx.commands.run(cmd, timeout=timeout)
        res = CmdResult(cmd, r.exit_code, r.stdout or "", r.stderr or "")
    except Exception as exc:  # CommandExitException (non-zero exit) or transport error
        if CommandExitException is not None and isinstance(exc, CommandExitException):
            res = CmdResult(cmd, getattr(exc, "exit_code", 1), getattr(exc, "stdout", "") or "",
                            getattr(exc, "stderr", "") or str(exc))
        else:
            raise
    if check and res.exit_code != 0:
        raise RuntimeError(f"command failed (exit {res.exit_code}): {cmd}\n{res.stderr[-1500:]}")
    return res


def provision(sbx, *, use_template: bool) -> None:
    """Prepare the sandbox to mount and run the agent.

    The slow, stable pieces (FUSE, the everest binary, python deps) are installed at
    runtime only when no prebaked template provides them. The agent package itself is
    *always* uploaded fresh from the repo, so a baked template can never ship stale
    agent code.
    """
    if not use_template:
        if not EVEREST_BIN.exists():
            raise FileNotFoundError(
                f"everest binary not found at {EVEREST_BIN}. Download the Linux x86_64 build and "
                "extract it there (see README)."
            )
        # FUSE userspace (baked into the template otherwise)
        run(sbx, "sudo apt-get update -qq && sudo apt-get install -y -qq fuse3", timeout=300)
        run(sbx, "echo user_allow_other | sudo tee -a /etc/fuse.conf >/dev/null", check=False)
        # everest binary
        sbx.files.write("/home/user/everest", EVEREST_BIN.read_bytes())
        run(sbx, "sudo mv /home/user/everest /usr/local/bin/everest && sudo chmod +x /usr/local/bin/everest && everest --version")
        # python deps for the agent — system-wide (sudo) so root can import them
        run(sbx, f"sudo pip install -q {PIP_DEPS}", timeout=420)

    # /dev/fuse is a per-sandbox device, not part of the image — relax its perms every
    # run so a non-root mount works (template or runtime-install path alike).
    run(sbx, "sudo chmod 666 /dev/fuse", check=False)

    # agent package modules — always uploaded fresh
    run(sbx, f"mkdir -p {SANDBOX_PKG_ROOT}/mount_receipts")
    for mod in SANDBOX_MODULES:
        sbx.files.write(f"{SANDBOX_PKG_ROOT}/mount_receipts/{mod}", (PKG_DIR / mod).read_text(encoding="utf-8"))


# Everything that touches the mount runs as root (sudo -E): the FUSE mount must be
# root-owned so E2B's root-owned envd (which serves the dashboard Filesystem tab) can see
# it; a user-owned everest mount is invisible to root, and everest has no allow_other.
def mount(sbx, repo: str, branch: str, *, mount_dir: str = MOUNT_DIR) -> None:
    run(sbx, f"sudo mkdir -p {mount_dir}")
    run(
        sbx,
        f"sudo -E everest mount lakefs://{repo}/{branch}/ {mount_dir} --protocol fuse --write-mode",
        timeout=180,
    )


def run_phase(sbx, phase: str, *, mount_dir: str = MOUNT_DIR) -> CmdResult:
    return run(
        sbx,
        f"sudo -E env PYTHONPATH={SANDBOX_PKG_ROOT} python -m mount_receipts.agent_runner {phase} {mount_dir}",
        timeout=420,
        check=False,  # validate phase returns exit 1 on a failed ledger; caller inspects result file
    )


def commit(sbx, msg: str, *, mount_dir: str = MOUNT_DIR) -> CmdResult:
    """Commit mounted changes. A phase that produced no diff is a no-op, not an error."""
    r = run(sbx, f'sudo -E everest commit {mount_dir} -m "{msg}"', timeout=180, check=False)
    if r.exit_code != 0 and "no changes" not in (r.stdout + r.stderr).lower():
        raise RuntimeError(f"everest commit failed: {r.stderr[-1500:]}")
    return r


def read_mount_file(sbx, rel_path: str, *, mount_dir: str = MOUNT_DIR) -> str:
    return run(sbx, f"sudo cat {mount_dir}/{rel_path}").stdout


def umount(sbx, *, mount_dir: str = MOUNT_DIR) -> None:
    run(sbx, f"sudo -E everest umount {mount_dir}", check=False)
