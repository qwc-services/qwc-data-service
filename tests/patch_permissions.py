#!/usr/bin/python3

import json
import sys

if len(sys.argv) < 2:
    print("Usage: %s permissions.json" % sys.argv[0], file=sys.stderr)
    sys.exit(1)

filename = sys.argv[1]

with open(filename, 'r') as fh:
    data = json.load(fh)

for role in data["roles"]:
    if role["role"] == "admin":
        for dataset in role["permissions"]["data_datasets"]:
            if dataset["name"] == "qwc_demo.edit_points":
                dataset["writable"] = False
                dataset["creatable"] = False
                dataset["readable"] = False
                dataset["readable"] = True
                dataset["updatable"] = False
                dataset["deletable"] = False
                break
        break

with open(filename, 'w') as fh:
    json.dump(data, fh, indent=2)
