"""
Reads a docker-logs dump from the AI Agent container and reports
the analysis-duration statistics printed by agent.py's:
    print(f"Analysis took {duration:.2f} seconds (provider: {ACTIVE_PROVIDER})")

Usage:
    python parse_agent_timings.py agent_log_gemini.txt
"""
import argparse
import re
import statistics


PATTERN = re.compile(r"Analysis took ([\d.]+) seconds \(provider: (\w+)\)")


def print_stats(label, durations):
    if not durations:
        print(f"{label}: no data")
        return
    durations = sorted(durations)
    p95_index = max(0, int(len(durations) * 0.95) - 1)
    print(f"[{label}]")
    print(f"  Count:   {len(durations)}")
    print(f"  Min:     {min(durations):.2f}s")
    print(f"  Max:     {max(durations):.2f}s")
    print(f"  Average: {statistics.mean(durations):.2f}s")
    print(f"  Median:  {statistics.median(durations):.2f}s")
    print(f"  P95:     {durations[p95_index]:.2f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("logfile", help="Path to a file saved via: docker logs --timestamps agent > file.txt")
    args = parser.parse_args()

    durations = []
    provider = None
    with open(args.logfile, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = PATTERN.search(line)
            if m:
                durations.append(float(m.group(1)))
                provider = m.group(2)

    if not durations:
        print("No 'Analysis took ...' lines found in this log file.")
        return

    print(f"Provider (from log): {provider}\n")

    print_stats("All successful analyses", durations)

    if len(durations) > 1:
        print()
        without_worst = sorted(durations)[:-1]
        print_stats("Excluding the single worst-case outlier", without_worst)


if __name__ == "__main__":
    main()