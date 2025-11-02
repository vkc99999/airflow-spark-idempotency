import json
import subprocess
from typing import List, Tuple

def _run_cmd(args: List[str]) -> Tuple[int, str, str]:
    """Run shell command with minimal logging noise."""
    if "kubectl" in args:
        p = subprocess.run(args, capture_output=True, text=True)
        return p.returncode, p.stdout.strip(), p.stderr.strip()

    p = subprocess.run(args, capture_output=True, text=True)
    print(f"[DBG] CMD: {' '.join(args)} | RC={p.returncode}")
    if p.stdout.strip():
        out_lines = p.stdout.splitlines()[:5]
        print(f"[DBG] OUT (first 5): {' | '.join(out_lines)}")
    if p.stderr.strip():
        err_lines = p.stderr.splitlines()[:3]
        print(f"[DBG] ERR (first 3): {' | '.join(err_lines)}")
    return p.returncode, p.stdout.strip(), p.stderr.strip()


def _first_or_blank(names: str) -> str:
    parts = [n for n in names.split() if n]
    return parts[0] if parts else ""


def _selector_driver_with_role(app_name: str, ns: str) -> str:
    rc, out, _ = _run_cmd([
        "kubectl", "get", "pods", "-n", ns,
        "-l", f"spark-app-name={app_name},spark-role=driver",
        "-o", "jsonpath={.items[*].metadata.name}",
    ])
    if rc == 0 and out:
        return _first_or_blank(out)
    return ""


def _selector_grep_driver_suffix(app_name: str, ns: str) -> str:
    rc, out, _ = _run_cmd([
        "kubectl", "get", "pods", "-n", ns,
        "-l", f"spark-app-name={app_name}",
        "-o", "jsonpath={.items[*].metadata.name}",
    ])
    if rc == 0 and out:
        for n in out.split():
            if n.endswith("-driver"):
                return n
    return ""


def _find_driver_once(app_name: str, ns: str) -> str:
    """Find one driver pod by role or suffix."""
    return _selector_driver_with_role(app_name, ns) or _selector_grep_driver_suffix(app_name, ns)


def _list_driver_pods(app_name: str, ns: str) -> List[str]:
    """List non-completed driver pods for the given Spark app."""
    rc, out, _ = _run_cmd([
        "kubectl", "get", "pods", "-n", ns,
        "-l", f"spark-app-name={app_name},spark-role=driver",
        "-o", "json",
    ])
    if rc != 0 or not out:
        return []
    try:
        data = json.loads(out)
        pods = []
        for item in data.get("items", []):
            name = item["metadata"]["name"]
            phase = item["status"].get("phase", "Unknown")
            if phase.lower() not in ("succeeded", "completed"):
                pods.append(name)
        pods.sort(
            key=lambda x: next(
                (
                    i["metadata"]["creationTimestamp"]
                    for i in data["items"]
                    if i["metadata"]["name"] == x
                ),
                "",
            ),
            reverse=True,
        )
        return pods
    except Exception as e:
        print(f"[DBG] parse driver list failed: {e}")
        return []


def _pod_phase(pod: str, ns: str) -> str:
    """Return pod phase from kubectl JSON output."""
    rc, out, _ = _run_cmd(["kubectl", "get", "pod", pod, "-n", ns, "-o", "json"])
    if rc == 0 and out:
        try:
            return json.loads(out)["status"]["phase"]
        except Exception as e:
            print(f"[DBG] parse pod JSON failed for {pod}: {e}")
    return "Unknown"


def _sanitize_psql_scalar(raw: str) -> str:
    """
    Normalize psql scalar output by stripping quotes, pipes, and whitespace.
    Example: '"spark-driver-pod" |' -> 'spark-driver-pod'
    """
    if not raw:
        return ""
    lines = [l for l in raw.splitlines() if l.strip()]
    if not lines:
        return ""
    s = lines[-1]
    return s.strip().strip("|").strip().strip('"').strip("'")
