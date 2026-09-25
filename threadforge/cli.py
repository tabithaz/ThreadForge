"""Minimal ThreadForge CLI."""
import argparse
import json
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    submit = sub.add_parser("submit")
    submit.add_argument("--task", choices=["echo", "sleep"], required=True)
    submit.add_argument("--text", default="")
    submit.add_argument("--seconds", type=float, default=0)
    submit.add_argument("--depends-on", action="append", default=[])
    submit.add_argument("--priority", type=int, default=0)
    sub.add_parser("list")
    status = sub.add_parser("status")
    status.add_argument("id")
    args = parser.parse_args()
    url = "http://127.0.0.1:8765"
    if args.command == "submit":
        payload = {"text": args.text} if args.task == "echo" else {"seconds": args.seconds}
        request = urllib.request.Request(url + "/jobs", data=json.dumps({
            "task": args.task, "payload": payload, "dependencies": args.depends_on,
            "priority": args.priority}).encode(), headers={"Content-Type": "application/json"})
    else:
        request = url + ("/jobs" if args.command == "list" else "/jobs/" + args.id)
    with urllib.request.urlopen(request) as response:
        print(json.dumps(json.load(response), indent=2))


if __name__ == "__main__":
    main()
