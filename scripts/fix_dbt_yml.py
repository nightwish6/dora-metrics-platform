#!/usr/bin/env python3
"""Fix deprecated accepted_values test arguments for dbt 2.0."""

import glob

files = glob.glob("/Users/stoyanovkhl/BIprojects/dora-metrics-platform/dbt/models/1_raw/*.yml")

for path in files:
    with open(path) as f:
        lines = f.readlines()

    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        new_lines.append(line)

        # Check if this is an accepted_values line
        stripped = line.rstrip()
        if stripped.endswith("accepted_values:"):
            # Next line should be "values: [...]"
            i += 1
            if i < len(lines):
                values_line = lines[i].rstrip()
                if values_line.strip().startswith("values:"):
                    # Get indentation of the line after accepted_values
                    indent = " " * (len(lines[i]) - len(lines[i].lstrip()))
                    new_lines.append(f"{indent}arguments:\n")
                    new_lines.append(f"{indent}  values:\n")
                    # Skip the old "values: [...]" line - read its content
                    content = values_line.split("values:")[1] if "values:" in values_line else ""
                    if content.strip():
                        new_lines.append(f"{indent}  {content}\n")
                    i += 1
                    continue

        i += 1

    with open(path, "w") as f:
        f.writelines(new_lines)
    print(f"Fixed {path}")

print("Done")
