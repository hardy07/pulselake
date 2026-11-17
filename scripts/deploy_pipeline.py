"""Validate and deploy the PulseLake job, then submit its first run."""
import argparse
import subprocess
import os
from pathlib import Path

from project_config import configure_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate-only", action="store_true",
                        help="Validate the bundle without deploying or starting a run")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    cli = configure_environment(root, require_connection=True)
    if not cli:
        parser.error("Install the Databricks CLI and ensure it is on PATH")
    common = ["--profile", os.environ["DATABRICKS_CONFIG_PROFILE"], "--target", "dev"]
    steps = [("Validate pipeline configuration", ["validate"])]
    if not args.validate_only:
        steps.extend([
            ("Deploy notebooks, dbt models and job", ["deploy", "--fail-on-active-runs"]),
            ("Submit setup -> Bronze -> Silver -> dbt", ["run", "pulselake_hourly", "--no-wait"]),
        ])
    for label, command in steps:
        print(f"\n{label}", flush=True)
        result = subprocess.run([str(cli), "bundle", *command, *common], cwd=root, check=False)
        if result.returncode:
            print(f"\nStopped: {label} failed. Fix the reported error before retrying.", flush=True)
            return result.returncode
    if args.validate_only:
        print("\nBundle validation passed.", flush=True)
    else:
        print("\nRun submitted, not yet verified successful. Open Jobs & Pipelines and find PulseLake Pipeline.", flush=True)
        print("After all four tasks succeed, set schedule.pause_status to UNPAUSED in databricks.yml and redeploy.", flush=True)
        print("Keep any UI edits in sync with the bundle before redeploying.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
