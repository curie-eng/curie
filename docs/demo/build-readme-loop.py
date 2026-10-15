"""Render the README's two concept loops as a small, self-contained GIF.

Requires rsvg-convert and ffmpeg. Run from any directory:

    python3 docs/demo/build-readme-loop.py

The first frame carries the whole explanation for readers who pause animation.
"""

from __future__ import annotations

import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / "co-worker-factory-loop.gif"
STILL = HERE / "co-worker-factory-still.png"
WIDTH = 960
HEIGHT = 476
FRAMES = 20

BG = "#0b1220"
PANEL = "#111d30"
CARD = "#182842"
EDGE = "#35506d"
RAIL = "#425b74"
WHITE = "#f3f7ff"
MUTED = "#a8bed2"
TEAL = "#52dec0"
AMBER = "#ffce7a"
BLUE = "#79aaff"


def rect(x, y, width, height, fill, radius=0, stroke="none", stroke_width=1):
    return (
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" '
        f'rx="{radius}" fill="{fill}" stroke="{stroke}" '
        f'stroke-width="{stroke_width}"/>'
    )


def line(x1, y1, x2, y2, color, width=2, dash=""):
    extra = f' stroke-dasharray="{dash}"' if dash else ""
    return (
        f'<path d="M {x1} {y1} L {x2} {y2}" fill="none" '
        f'stroke="{color}" stroke-width="{width}" stroke-linecap="round"{extra}/>'
    )


def text(x, y, value, size=20, color=WHITE, weight=500, anchor="start"):
    return (
        f'<text x="{x}" y="{y}" fill="{color}" font-family="Arial, Helvetica, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}">'
        f"{escape(value)}</text>"
    )


def circle(x, y, radius, fill, stroke="none", stroke_width=1):
    return (
        f'<circle cx="{x}" cy="{y}" r="{radius}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{stroke_width}"/>'
    )


def card(x, y, width, height, accent):
    return "".join(
        [
            rect(x, y + 6, width, height, "#08111e", 13),
            rect(x, y, width, height, CARD, 13, EDGE),
            rect(x, y, 5, height, accent, 2),
        ]
    )


def pill(x, y, width, label, color):
    return rect(x, y, width, 24, "#24354c", 12) + text(
        x + width / 2, y + 16, label, 12, color, 700, "middle"
    )


def eased(a):
    a = min(1.0, max(0.0, a))
    return a * a * (3 - 2 * a)


def travel(t, start, end, x1=259, x2=700):
    return x1 + (x2 - x1) * eased((t - start) / (end - start))


def station(x, y, label, highlight):
    return "".join(
        [
            line(x, y - 12, x, y + 12, highlight, 3),
            circle(x, y, 5, highlight),
            text(x, y + 37, label, 12, MUTED, 600, "middle"),
        ]
    )


def frame(index):
    t = index / FRAMES
    top_x = travel(t, 0.05, 0.72)
    bottom_x = travel(t, 0.15, 0.82)
    top_active = 0.4 + 0.6 * max(0, math.sin(math.pi * eased((t - 0.17) / 0.5)))
    bottom_active = 0.4 + 0.6 * max(0, math.sin(math.pi * eased((t - 0.28) / 0.5)))
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}">',
        rect(0, 0, WIDTH, HEIGHT, BG, 18),
        rect(16, 14, 928, 215, PANEL, 20),
        rect(16, 244, 928, 215, PANEL, 20),
        text(40, 47, "01", 15, TEAL, 700),
        text(80, 48, "From existing agent to co-worker", 21, WHITE, 700),
        text(40, 277, "02", 15, AMBER, 700),
        text(80, 278, "Software Factory: issue to reviewable change", 21, WHITE, 700),
        # Both rows have a real reading order even when the animation is still.
        card(40, 67, 224, 102, TEAL),
        text(61, 102, "Adapt your agent", 18, WHITE, 700),
        text(61, 127, "Skills + MCP tools", 15, MUTED),
        pill(61, 138, 93, "MCP tools", TEAL),
        card(330, 67, 300, 102, BLUE),
        text(351, 102, "Curie", 18, WHITE, 700),
        text(351, 127, "Configured workspace controls", 15, MUTED),
        pill(351, 138, 82, "Approval", BLUE),
        pill(440, 138, 94, "Scoped facts", BLUE),
        pill(541, 138, 68, "Model", BLUE),
        card(696, 67, 224, 102, TEAL),
        text(717, 102, "Slack co-worker", 18, WHITE, 700),
        text(717, 127, "Reply + gated actions", 15, MUTED),
        pill(717, 138, 126, "In your workspace", TEAL),
        card(40, 297, 224, 102, AMBER),
        text(61, 332, "Supported issue", 18, WHITE, 700),
        text(61, 357, "GitHub / GitLab / Jira Cloud", 14, MUTED),
        pill(61, 368, 91, "Ticket", AMBER),
        card(330, 297, 300, 102, BLUE),
        text(351, 332, "Factory run", 18, WHITE, 700),
        text(351, 357, "Implement + agent review", 15, MUTED),
        pill(351, 368, 99, "Isolated run", BLUE),
        pill(458, 368, 93, "Review", BLUE),
        card(696, 297, 224, 102, AMBER),
        text(717, 332, "PR / MR", 18, WHITE, 700),
        text(717, 357, "Team inspects result", 15, MUTED),
        pill(717, 368, 95, "CI + result", AMBER),
        # A shared track makes the two workflows feel like working machinery.
        line(258, 193, 700, 193, RAIL, 6),
        line(258, 423, 700, 423, RAIL, 6),
        line(258, 193, round(top_x, 1), 193, TEAL, 6),
        line(258, 423, round(bottom_x, 1), 423, AMBER, 6),
        station(264, 193, "REUSE", TEAL),
        station(480, 193, "GOVERN", BLUE),
        station(696, 193, "COLLABORATE", TEAL),
        station(264, 423, "DELEGATE", AMBER),
        station(480, 423, "BUILD + REVIEW", BLUE),
        station(696, 423, "TEAM DECIDES", AMBER),
        circle(round(top_x, 1), 193, 15, "#123f40", TEAL, 3),
        circle(round(top_x, 1), 193, 5, TEAL),
        rect(round(bottom_x - 15, 1), 408, 30, 30, "#614322", 7, AMBER, 3),
        line(round(bottom_x - 6, 1), 418, round(bottom_x + 6, 1), 418, AMBER, 2),
        line(round(bottom_x - 6, 1), 426, round(bottom_x + 3, 1), 426, AMBER, 2),
        circle(480, 73, 3 + round(top_active * 2, 1), TEAL),
        circle(480, 303, 3 + round(bottom_active * 2, 1), AMBER),
        "</svg>",
    ]
    return "".join(parts)


def main():
    with tempfile.TemporaryDirectory(prefix="curie-readme-loop-") as temp:
        temp_path = Path(temp)
        for index in range(FRAMES):
            source = temp_path / f"frame{index:03d}.svg"
            image = temp_path / f"frame{index:03d}.png"
            source.write_text(frame(index))
            subprocess.run(["rsvg-convert", str(source), "-o", str(image)], check=True)
        shutil.copyfile(temp_path / "frame000.png", STILL)
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-framerate",
                "4",
                "-i",
                str(temp_path / "frame%03d.png"),
                "-filter_complex",
                "[0:v]split[a][b];[a]palettegen=max_colors=80[p];"
                "[b][p]paletteuse=dither=bayer:bayer_scale=4",
                # Play once, then hold the last explanatory frame.
                "-loop",
                "-1",
                str(OUTPUT),
            ],
            check=True,
        )
    print(OUTPUT)
    print(STILL)


if __name__ == "__main__":
    main()
