#!/usr/bin/env python3
"""
Standalone check for the network-rebounce feature (no boto3 / camera deps).

Verifies, on the device, that the configured NetworkManager connection exists and
that `sudo nmcli connection down/up <conn>` can actually run — before you flip
network_rebounce_enabled on in config.conf.

Usage:
    python3 test_rebounce.py                 # read-only checks (config, nmcli, connection, sudo)
    python3 test_rebounce.py --run           # ALSO perform a real down/up bounce (drops the link briefly!)
    python3 test_rebounce.py --config x.conf # use a different config file
"""

import argparse
import configparser
import shutil
import subprocess
import sys
import time


def _load(config_file):
    cfg = configparser.ConfigParser()
    if not cfg.read(config_file):
        print(f"FAIL  could not read config file: {config_file}")
        sys.exit(2)
    g = lambda k, d: cfg.get("recording", k, fallback=d)
    return {
        "enabled": cfg.getboolean("recording", "network_rebounce_enabled", fallback=False),
        "conn": g("network_rebounce_connection", "main-pi").strip(),
        "cooldown": float(g("network_rebounce_cooldown_sec", "120")),
        "wait_after_up": float(g("network_rebounce_wait_after_up_sec", "15")),
        "down_up_gap": float(g("network_rebounce_down_up_gap_sec", "2")),
        "cmd_timeout": float(g("network_rebounce_cmd_timeout_sec", "30")),
    }


def _run(cmd, timeout):
    print(f"      $ {' '.join(cmd)}")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return None, "", "command not found"
    except subprocess.TimeoutExpired:
        return None, "", f"timed out after {timeout}s"
    except Exception as e:  # noqa: BLE001
        return None, "", str(e)
    return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()


def main():
    ap = argparse.ArgumentParser(description="Verify the nmcli network-rebounce setup")
    ap.add_argument("--config", default="config.conf")
    ap.add_argument("--run", action="store_true", help="Actually bounce the link (drops the connection briefly)")
    args = ap.parse_args()

    c = _load(args.config)
    print("=== Network rebounce check ===")
    print(f"config file         : {args.config}")
    print(f"feature enabled     : {c['enabled']}  (network_rebounce_enabled)")
    print(f"connection name     : {c['conn']}")
    print(f"cooldown / gap / up : {c['cooldown']:.0f}s / {c['down_up_gap']:.0f}s / {c['wait_after_up']:.0f}s")
    print(f"cmd timeout         : {c['cmd_timeout']:.0f}s")
    print()

    ok = True

    # 1. nmcli present?
    if shutil.which("nmcli") is None:
        print("FAIL  nmcli not found in PATH (this script is meant to run on the Pi)")
        return 1
    print("OK    nmcli found")

    # 2. connection exists?
    rc, out, err = _run(["nmcli", "-t", "-f", "NAME", "connection", "show"], c["cmd_timeout"])
    names = [n for n in (out or "").splitlines() if n]
    if rc != 0:
        print(f"WARN  could not list connections (rc={rc}): {err}")
        ok = False
    elif c["conn"] in names:
        print(f"OK    connection '{c['conn']}' exists")
    else:
        print(f"FAIL  connection '{c['conn']}' NOT found. Available: {', '.join(names) or '(none)'}")
        print("      Fix network_rebounce_connection in config.conf to one of the above.")
        ok = False

    # 3. passwordless sudo for nmcli? (-n = non-interactive: fails instead of prompting)
    rc, out, err = _run(["sudo", "-n", "nmcli", "connection", "show", c["conn"]], c["cmd_timeout"])
    if rc == 0:
        print("OK    passwordless sudo for nmcli works")
    else:
        print(f"FAIL  `sudo -n nmcli ...` failed (rc={rc}): {err or 'requires a password / not permitted'}")
        print("      Add a sudoers drop-in so the service user can bounce without a password, e.g.:")
        print(f"        <user> ALL=(root) NOPASSWD: /usr/bin/nmcli connection up {c['conn']}, "
              f"/usr/bin/nmcli connection down {c['conn']}")
        ok = False

    if not c["enabled"]:
        print("\nNOTE  network_rebounce_enabled = false — set it to true to activate the feature.")

    # 4. optional: actually bounce
    if args.run:
        if not ok:
            print("\nSkipping --run bounce because the checks above failed.")
            return 1
        print(f"\n--run: bouncing '{c['conn']}' now (link will drop briefly)...")
        rc_d, _, err_d = _run(["sudo", "-n", "nmcli", "connection", "down", c["conn"]], c["cmd_timeout"])
        print(f"      down rc={rc_d} {err_d}")
        time.sleep(c["down_up_gap"])
        rc_u, _, err_u = _run(["sudo", "-n", "nmcli", "connection", "up", c["conn"]], c["cmd_timeout"])
        print(f"      up   rc={rc_u} {err_u}")
        time.sleep(c["wait_after_up"])
        if rc_u == 0:
            print(f"OK    bounce completed; '{c['conn']}' is back up")
        else:
            print(f"FAIL  'up' returned rc={rc_u} — connection may be down, check `nmcli connection show --active`")
            return 1

    print("\nAll checks passed." if ok else "\nOne or more checks FAILED — see above.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
