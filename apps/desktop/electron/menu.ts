// The application menu, cut down to what an operator console needs.
//
// Electron's default menu is a browser's menu: reload, zoom, view source,
// toggle full screen, and a Window menu built for many windows. Most of that is
// noise in an app with one document, and some of it (Reload) is actively
// confusing next to a running `curie local up`. What survives is the editing
// commands people expect from muscle memory, plus the app's own navigation.

import { app, Menu, shell, type BrowserWindow, type MenuItemConstructorOptions } from "electron";

const DOCS = "https://github.com/curie-eng/curie";

/** Ask the renderer to move; the renderer owns routing, the menu only nudges. */
function go(getWin: () => BrowserWindow | null, route: string) {
  return () => getWin()?.webContents.send("curie:navigate", route);
}

export function buildMenu(getWin: () => BrowserWindow | null): Menu {
  const isMac = process.platform === "darwin";

  const template: MenuItemConstructorOptions[] = [
    ...(isMac
      ? ([
          {
            label: app.getName(),
            submenu: [
              { role: "about" },
              { type: "separator" },
              { label: "Settings…", accelerator: "Cmd+,", click: go(getWin, "settings") },
              { type: "separator" },
              { role: "services" },
              { type: "separator" },
              { role: "hide" },
              { role: "hideOthers" },
              { type: "separator" },
              { role: "quit" },
            ],
          },
        ] as MenuItemConstructorOptions[])
      : []),
    {
      label: "File",
      submenu: [
        { label: "Open Bundle…", accelerator: "CmdOrCtrl+O", click: go(getWin, "workspace:open") },
        { type: "separator" },
        { label: "New Agent…", accelerator: "CmdOrCtrl+N", click: go(getWin, "commands:init") },
        { type: "separator" },
        isMac ? { role: "close" } : { role: "quit" },
      ],
    },
    {
      label: "Edit",
      submenu: [
        { role: "undo" },
        { role: "redo" },
        { type: "separator" },
        { role: "cut" },
        { role: "copy" },
        { role: "paste" },
        { role: "selectAll" },
      ],
    },
    {
      label: "Go",
      submenu: [
        { label: "Command Palette", accelerator: "CmdOrCtrl+K", click: go(getWin, "palette") },
        { type: "separator" },
        { label: "Overview", accelerator: "CmdOrCtrl+1", click: go(getWin, "overview") },
        { label: "Build", accelerator: "CmdOrCtrl+2", click: go(getWin, "build") },
        { label: "Tiers", accelerator: "CmdOrCtrl+3", click: go(getWin, "tiers") },
        { label: "Resources", accelerator: "CmdOrCtrl+4", click: go(getWin, "resources") },
        { label: "Canvas", accelerator: "CmdOrCtrl+5", click: go(getWin, "canvas") },
        { type: "separator" },
        // Two panes of one tab, so they sit together and only the tab gets a
        // number -- matching the single row the sidebar draws for both.
        { label: "Commands", accelerator: "CmdOrCtrl+6", click: go(getWin, "commands") },
        { label: "Command History", click: go(getWin, "activity") },
      ],
    },
    {
      label: "View",
      submenu: [
        // Reload survives only outside a packaged build: mid-`local up` it would
        // orphan the transcript the operator is reading.
        ...(app.isPackaged
          ? []
          : ([{ role: "reload" }, { role: "toggleDevTools" }, { type: "separator" }] as MenuItemConstructorOptions[])),
        { role: "resetZoom" },
        { role: "zoomIn" },
        { role: "zoomOut" },
        { type: "separator" },
        { role: "togglefullscreen" },
      ],
    },
    {
      role: "help",
      submenu: [
        { label: "Curie Documentation", click: () => void shell.openExternal(DOCS) },
        { label: "CLI Reference", click: go(getWin, "commands") },
      ],
    },
  ];

  return Menu.buildFromTemplate(template);
}
