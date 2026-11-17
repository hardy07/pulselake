"""Build and verify Gold using the signed-in user's Databricks CLI session."""
import argparse
import os
import subprocess
import sys
import shutil
from pathlib import Path
from project_config import configure_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parse-only", action="store_true", help="Validate project configuration without connecting")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    configure_environment(root, require_connection=not args.parse_only)
    dbt = shutil.which("dbt")
    if not dbt:
        parser.error("Install requirements-dbt.txt into your active Python environment first")
    dbt_args = ["--project-dir", str(root / "dbt"), "--profiles-dir", str(root / "dbt"),
                "--log-path", str(root / "dbt" / "logs")]
    # Only commands that produce model artifacts accept --target-path.
    model_args = [*dbt_args, "--target-path", str(root / "dbt" / "target")]
    verifier = root / "scripts" / "verify_lakehouse.py"
    steps = [("Parse dbt project", [str(dbt), "parse", *model_args])]
    if not args.parse_only:
        steps.extend([
            ("Verify Bronze and Silver", [sys.executable, "-u", str(verifier)]),
            ("Check dbt warehouse connection", [str(dbt), "debug", *dbt_args]),
            ("Build Gold models and run data tests", [str(dbt), "build", *model_args]),
            ("Verify Gold tables", [sys.executable, "-u", str(verifier), "--gold"]),
        ])
    for label, command in steps:
        print(f"\n{label}", flush=True)
        result = subprocess.run(command, cwd=root, check=False)
        if result.returncode:
            print(f"\nStopped: {label} failed. Fix the reported error before retrying.", flush=True)
            return result.returncode
    print("\nProject parse passed." if args.parse_only else "\nGold build and verification completed successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
