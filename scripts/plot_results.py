#!/usr/bin/env python3
"""Turn the captured benchmark/crash logs into the SVG charts in docs/img.

Everything plotted is parsed out of docs/results/*.txt, so the charts cannot
drift away from the numbers that were actually measured.

    python3 scripts/plot_results.py
"""

import math
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
RESULTS = ROOT / "docs" / "results"
IMG = ROOT / "docs" / "img"

# Light defaults, overridden for dark mode where the renderer honours the media
# query. The background is always painted, so a stripped media query degrades
# to a readable light card rather than dark-on-dark.
STYLE = """
  <style>
    .bg    { fill: #f6f8fa; stroke: #d0d7de; }
    .title { fill: #1f2328; font: 600 17px -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; }
    .sub   { fill: #656d76; font: 400 12.5px -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; }
    .label { fill: #1f2328; font: 400 13px -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; }
    .value { fill: #1f2328; font: 600 13px ui-monospace, SFMono-Regular, Menlo, monospace; }
    .axis  { fill: #656d76; font: 400 11px ui-monospace, SFMono-Regular, Menlo, monospace; }
    .grid  { stroke: #d8dee4; stroke-width: 1; }
    .zero  { stroke: #8c959f; stroke-width: 1.5; }
    .big   { fill: #1f2328; font: 700 38px -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; }
    @media (prefers-color-scheme: dark) {
      .bg    { fill: #0d1117; stroke: #30363d; }
      .title, .label, .value, .big { fill: #e6edf3; }
      .sub, .axis { fill: #8b949e; }
      .grid  { stroke: #21262d; }
      .zero  { stroke: #6e7681; }
    }
  </style>
"""

BLUE, GREEN, AMBER, PURPLE = "#2f81f7", "#2ea043", "#d29922", "#8957e5"


def esc(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def svg(width, height, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">{STYLE}'
        f'<rect class="bg" x="0.5" y="0.5" width="{width - 1}" height="{height - 1}" rx="8"/>'
        f"{body}</svg>\n"
    )


# --------------------------------------------------------------------- parsing

def read(name):
    path = RESULTS / name
    if not path.exists():
        sys.exit(f"missing {path}; run the benchmarks first (see README)")
    return path.read_text()


def ops(text, section, label):
    """Pull '<label> : <n>K ops/sec' out of the given [section] of a log."""
    chunk = text.split(f"[{section}]", 1)
    if len(chunk) < 2:
        sys.exit(f"no [{section}] section found")
    body = re.split(r"\n\[", chunk[1])[0]
    m = re.search(rf"^\s*{re.escape(label)}\s*:\s*([\d.]+)(K?) ops/sec", body, re.M)
    if not m:
        sys.exit(f"no '{label}' line in [{section}]")
    return float(m.group(1)) * (1000 if m.group(2) else 1)


def summary_percentages(text):
    body = text.split("[summary]", 1)[1]
    out = {}
    for label, pct in re.findall(r"^\s*(.+?)\s*:\s*([-+][\d.]+)%", body, re.M):
        out[label.strip()] = float(pct)
    return out


def crash_numbers(text):
    m = re.search(r"(\d+) crash cycles in ([\d.]+)s, (\d+) acknowledged writes verified, (\d+) lost", text)
    if not m:
        sys.exit("could not find the crash-test summary line")
    return int(m.group(1)), float(m.group(2)), int(m.group(3)), int(m.group(4))


# ---------------------------------------------------------------------- charts

def log_bars(path, title, subtitle, rows):
    """Horizontal bars on a log scale, for values spanning three decades."""
    pad, label_w, row_h = 22, 268, 34
    width, head = 900, 74
    x0 = pad + label_w
    x1 = width - pad - 74
    lo, hi = 1_000.0, 2_000_000.0

    def x_of(v):
        v = max(v, lo)
        t = (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo))
        return x0 + t * (x1 - x0)

    height = head + row_h * len(rows) + 34
    parts = [
        f'<text class="title" x="{pad}" y="32">{esc(title)}</text>',
        f'<text class="sub" x="{pad}" y="54">{esc(subtitle)}</text>',
    ]
    for tick in (1_000, 10_000, 100_000, 1_000_000):
        tx = x_of(tick)
        parts.append(
            f'<line class="grid" x1="{tx:.1f}" y1="{head - 12}" x2="{tx:.1f}" y2="{head + row_h * len(rows)}"/>'
        )
        tick_label = f"{tick // 1000}K" if tick < 1_000_000 else "1M"
        parts.append(
            f'<text class="axis" x="{tx:.1f}" y="{head + row_h * len(rows) + 20}" text-anchor="middle">{tick_label}</text>'
        )
    for i, (label, value, colour) in enumerate(rows):
        y = head + i * row_h
        bar_w = max(x_of(value) - x0, 2)
        parts.append(f'<text class="label" x="{x0 - 12}" y="{y + 18}" text-anchor="end">{esc(label)}</text>')
        parts.append(f'<rect x="{x0}" y="{y + 5}" width="{bar_w:.1f}" height="18" rx="3" fill="{colour}"/>')
        shown = f"{value / 1000:.1f}K" if value >= 1000 else f"{value:.0f}"
        parts.append(f'<text class="value" x="{x0 + bar_w + 9:.1f}" y="{y + 19}">{shown}</text>')
    parts.append(
        f'<text class="axis" x="{(x0 + x1) / 2:.0f}" y="{height - 6}" text-anchor="middle">operations per second (log scale)</text>'
    )
    path.write_text(svg(width, height, "".join(parts)))


def diverging_bars(path, title, subtitle, rows):
    """Bars either side of zero: negative (left, green) means faster."""
    pad, label_w, row_h = 22, 268, 36
    width, head = 900, 74
    x0 = pad + label_w
    x1 = width - pad - 20
    lo, hi = -100.0, 20.0
    zero = x0 + (0 - lo) / (hi - lo) * (x1 - x0)

    def x_of(v):
        return x0 + (v - lo) / (hi - lo) * (x1 - x0)

    height = head + row_h * len(rows) + 34
    parts = [
        f'<text class="title" x="{pad}" y="32">{esc(title)}</text>',
        f'<text class="sub" x="{pad}" y="54">{esc(subtitle)}</text>',
    ]
    for tick in (-100, -75, -50, -25, 0, 20):
        tx = x_of(tick)
        cls = "zero" if tick == 0 else "grid"
        parts.append(f'<line class="{cls}" x1="{tx:.1f}" y1="{head - 12}" x2="{tx:.1f}" y2="{head + row_h * len(rows)}"/>')
        parts.append(f'<text class="axis" x="{tx:.1f}" y="{head + row_h * len(rows) + 20}" text-anchor="middle">{tick}%</text>')
    for i, (label, pct) in enumerate(rows):
        y = head + i * row_h
        tx = x_of(pct)
        left, bar_w = (min(tx, zero), abs(tx - zero))
        colour = GREEN if pct < 0 else AMBER
        parts.append(f'<text class="label" x="{x0 - 12}" y="{y + 20}" text-anchor="end">{esc(label)}</text>')
        parts.append(f'<rect x="{left:.1f}" y="{y + 6}" width="{max(bar_w, 2):.1f}" height="19" rx="3" fill="{colour}"/>')
        anchor, vx = ("end", left - 8) if pct < 0 else ("start", left + bar_w + 8)
        parts.append(f'<text class="value" x="{vx:.1f}" y="{y + 21}" text-anchor="{anchor}">{pct:+.1f}%</text>')
    parts.append(
        f'<text class="axis" x="{(x0 + x1) / 2:.0f}" y="{height - 6}" text-anchor="middle">change in mean read latency (negative = faster)</text>'
    )
    path.write_text(svg(width, height, "".join(parts)))


def stat_card(path, title, subtitle, stats):
    width, height = 900, 186
    parts = [
        f'<text class="title" x="22" y="32">{esc(title)}</text>',
        f'<text class="sub" x="22" y="54">{esc(subtitle)}</text>',
    ]
    box = (width - 44) / len(stats)
    for i, (value, caption, colour) in enumerate(stats):
        cx = 22 + box * i + box / 2
        parts.append(f'<text class="big" x="{cx:.0f}" y="122" text-anchor="middle" fill="{colour}">{esc(value)}</text>')
        parts.append(f'<text class="sub" x="{cx:.0f}" y="148" text-anchor="middle">{esc(caption)}</text>')
    path.write_text(svg(width, height, "".join(parts)))


# ------------------------------------------------------------------------ main

def main():
    IMG.mkdir(parents=True, exist_ok=True)
    three_m = read("bench-3m.txt")
    twenty_m = read("bench-20m-random.txt")
    durability = read("bench-durability.txt")
    crash = read("crash-1000.txt")

    fsync_single, fsync_batched = [
        ops(part, "load", "write throughput")
        for part in ("[load]" + p for p in durability.split("[load]")[1:])
    ]

    log_bars(
        IMG / "throughput.svg",
        "lsmkv throughput",
        "4-vCPU Linux cloud container, virtio disk, ext4 - 17-byte keys, 100-byte values",
        [
            ("Writes, buffered log, 3M keys", ops(three_m, "load", "write throughput"), BLUE),
            ("Writes, random order, 20M keys", ops(twenty_m, "load", "write throughput"), BLUE),
            ("Writes, fsync each, batches of 100", fsync_batched, PURPLE),
            ("Writes, fsync each, one at a time", fsync_single, PURPLE),
            ("Reads, 3M keys, uniform", ops(three_m, "read", "uniform, all hit"), GREEN),
            ("Reads, 20M keys, uniform", ops(twenty_m, "read", "uniform, all hit"), GREEN),
            ("Reads, 20M keys, hot 10%", ops(twenty_m, "read", "hot set, 20% miss"), GREEN),
            ("Reads, 3M keys, all misses", ops(three_m, "read", "uniform, all miss"), GREEN),
        ],
    )

    pcts = summary_percentages(three_m)
    diverging_bars(
        IMG / "bloom-cache.svg",
        "What the Bloom filters and block cache buy",
        "Same 3M-key dataset and the same reads, with bloom_bits_per_key = 0 and a 64 KB cache",
        [
            ("All misses", pcts["uniform, all miss"]),
            ("Hot set (90% of reads on 10% of keys)", pcts["hot set, 20% miss"]),
            ("50% misses", pcts["uniform, 50% miss"]),
            ("All hits, uniform", pcts["uniform, all hit"]),
        ],
    )

    cycles, seconds, verified, lost = crash_numbers(crash)
    stat_card(
        IMG / "crash.svg",
        "Crash test: SIGKILL at randomised moments, then reopen and verify",
        f"{cycles:,} kill/recover cycles in {seconds:.0f}s - every acknowledged write checked for presence and value",
        [
            (f"{cycles:,}", "crash cycles", BLUE),
            (f"{verified:,}", "acknowledged writes verified", BLUE),
            (str(lost), "lost", GREEN if lost == 0 else "#f85149"),
        ],
    )
    print("wrote", ", ".join(sorted(p.name for p in IMG.glob("*.svg"))))


if __name__ == "__main__":
    main()
