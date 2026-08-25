// The preload: the only script that sees both worlds.
//
// It exposes `window.curie` and nothing else. Every method is a fixed IPC
// channel with a fixed shape -- the renderer cannot name an arbitrary channel,
// cannot reach `ipcRenderer`, and cannot reach Node. Subscriptions hand back an
// unsubscribe function so React effects can clean up without the listener count
// growing across re-renders.

import { contextBridge, ipcRenderer } from "electron";
import { CH } from "./shared/contract.js";
import type {
  ApiRequest,
  CliInvocation,
  CurieBridge,
  ResourceFrame,
  RunChunk,
  RunResult,
} from "./shared/contract.js";

function subscribe<T>(channel: string, cb: (payload: T) => void): () => void {
  const handler = (_event: unknown, payload: T) => cb(payload);
  ipcRenderer.on(channel, handler);
  return () => ipcRenderer.removeListener(channel, handler);
}

const bridge: CurieBridge = {
  env: () => ipcRenderer.invoke(CH.env),

  cli: {
    preview: (inv: CliInvocation) => ipcRenderer.invoke(CH.cliPreview, inv),
    run: (inv: CliInvocation) => ipcRenderer.invoke(CH.cliRun, inv),
    cancel: (runId: string) => ipcRenderer.invoke(CH.cliCancel, runId),
    write: (runId: string, data: string) => ipcRenderer.invoke(CH.cliWrite, runId, data),
    onChunk: (cb: (chunk: RunChunk) => void) => subscribe(CH.cliChunk, cb),
    onResult: (cb: (result: RunResult) => void) => subscribe(CH.cliResult, cb),
  },

  resources: {
    start: (intervalMs: number) => ipcRenderer.invoke(CH.resStart, intervalMs),
    stop: () => ipcRenderer.invoke(CH.resStop),
    onFrame: (cb: (frame: ResourceFrame) => void) => subscribe(CH.resFrame, cb),
    logs: (id: string, tail: number) => ipcRenderer.invoke(CH.resLogs, id, tail),
  },

  workspace: {
    list: () => ipcRenderer.invoke(CH.wsList),
    open: () => ipcRenderer.invoke(CH.wsOpen),
    add: (path: string) => ipcRenderer.invoke(CH.wsAdd, path),
    forget: (path: string) => ipcRenderer.invoke(CH.wsForget, path),
    readFile: (root: string, rel: string) => ipcRenderer.invoke(CH.wsRead, root, rel),
    writeFile: (root: string, rel: string, body: string) =>
      ipcRenderer.invoke(CH.wsWrite, root, rel, body),
    revealInFileManager: (path: string) => ipcRenderer.invoke(CH.wsReveal, path),
  },

  api: {
    connection: () => ipcRenderer.invoke(CH.apiConnection),
    connect: (baseUrl: string, apiKey: string | null) =>
      ipcRenderer.invoke(CH.apiConnect, baseUrl, apiKey),
    request: (req: ApiRequest) => ipcRenderer.invoke(CH.apiRequest, req),
  },

  secrets: {
    list: () => ipcRenderer.invoke(CH.secList),
    set: (name: string, value: string) => ipcRenderer.invoke(CH.secSet, name, value),
    unset: (name: string) => ipcRenderer.invoke(CH.secUnset, name),
  },

  graph: {
    load: () => ipcRenderer.invoke(CH.graphLoad),
    save: (doc: unknown) => ipcRenderer.invoke(CH.graphSave, doc),
  },

  shell: {
    openExternal: (url: string) => ipcRenderer.invoke(CH.openExternal, url),
    copy: (text: string) => ipcRenderer.invoke(CH.copy, text),
  },
};

contextBridge.exposeInMainWorld("curie", bridge);

// Menu-driven navigation. One channel, one string payload; the renderer decides
// what a route means.
contextBridge.exposeInMainWorld("curieNav", {
  onNavigate: (cb: (route: string) => void) => subscribe<string>("curie:navigate", cb),
});
