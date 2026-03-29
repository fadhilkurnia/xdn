"""gp_config_utils.py — Helpers for GigaPaxos .properties file management.

GigaPaxos has two property-reading mechanisms:

1. System.getProperty() — works with JVM -D flags. Used by PB_*, HTTP_FORCE_KEEPALIVE, etc.
2. Config.getGlobalBoolean() — reads from .properties file and Config's internal cmdLine map;
   does NOT check System.getProperty().

Properties like SYNC and BATCHING_ENABLED use mechanism #2, so passing them as -D JVM flags
has no effect. This module provides apply_config_overrides() to extract such properties from
JVM args and write them into the .properties file before the cluster starts.
"""

import os
import re
import shutil
from pathlib import Path

# Properties read via Config.getGlobalBoolean() / Config.getGlobalInt() that
# must be set in the .properties file, not as -D JVM flags.
CONFIG_FRAMEWORK_KEYS = {"SYNC", "BATCHING_ENABLED"}


def apply_config_overrides(gp_jvm_args, config_file):
    """Extract Config-framework properties from JVM args into the .properties file.

    For each -DKEY=VALUE in gp_jvm_args where KEY is a Config-framework property
    (SYNC, BATCHING_ENABLED), this function:
      1. Updates the .properties file to set KEY=VALUE (replacing existing line or appending)
      2. Removes -DKEY=VALUE from the returned JVM args string

    Args:
        gp_jvm_args: JVM args string (e.g. "-DSYNC=true -DPB_N_PARALLEL_WORKERS=256")
        config_file: Path to the GigaPaxos .properties file

    Returns:
        Cleaned gp_jvm_args with Config-framework -D flags removed.
    """
    pattern = re.compile(r'-D(\w+)=(\S+)')
    overrides = {}
    tokens_to_remove = []

    for match in pattern.finditer(gp_jvm_args):
        key, value = match.group(1), match.group(2)
        if key in CONFIG_FRAMEWORK_KEYS:
            overrides[key] = value
            tokens_to_remove.append(match.group(0))

    if not overrides:
        return gp_jvm_args

    # Read, update, write the properties file
    with open(config_file, "r") as f:
        lines = f.readlines()

    keys_written = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        replaced = False
        for key, value in overrides.items():
            if re.match(rf"^#?\s*{key}\s*=", stripped):
                new_lines.append(f"{key}={value}\n")
                keys_written.add(key)
                replaced = True
                break
        if not replaced:
            new_lines.append(line)

    # Append any overrides that weren't already in the file
    for key, value in overrides.items():
        if key not in keys_written:
            new_lines.append(f"{key}={value}\n")

    with open(config_file, "w") as f:
        f.writelines(new_lines)

    # Remove the Config-framework -D flags from JVM args
    cleaned = gp_jvm_args
    for token in tokens_to_remove:
        cleaned = cleaned.replace(token, "")
    cleaned = " ".join(cleaned.split())  # normalize whitespace

    print(f"   [gp_config] Applied to {config_file}: {overrides}")
    return cleaned


def save_effective_config(config_file, gp_jvm_args, results_dir):
    """Copy the effective GigaPaxos config and JVM args into the results folder.

    Saves:
      - results_dir/gigapaxos_config.properties — copy of the .properties file
      - results_dir/jvm_args.txt — the JVM args string used for the run

    Args:
        config_file: Path to the GigaPaxos .properties file
        gp_jvm_args: JVM args string (after apply_config_overrides)
        results_dir: Path to the results directory (str or Path)
    """
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    dest_config = results_dir / "gigapaxos_config.properties"
    shutil.copy2(config_file, dest_config)

    dest_args = results_dir / "jvm_args.txt"
    with open(dest_args, "w") as f:
        f.write(gp_jvm_args + "\n")

    print(f"   [gp_config] Saved config -> {dest_config}")
