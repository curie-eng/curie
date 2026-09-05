# desktop-bridge

The contract between the Electron shell (`apps/desktop`) and the console it
hosts (`apps/ui`), declared once so the two builds cannot drift.

The shell injects `window.curie`. `apps/ui` reads it through `desktopBridge()`,
which returns `null` in a browser. That single check is the whole difference
between the web console and the desktop app: where a browser can only offer to
copy a `curie` command, the shell can run it.

Both sides import this file. Neither declares the shape for itself.
