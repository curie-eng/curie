from __future__ import annotations

import sys


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "recover":
        from .recover import main as recover_main

        raise SystemExit(recover_main(sys.argv[2:]))
    from .run import main as run_main

    run_main()


if __name__ == "__main__":
    main()
