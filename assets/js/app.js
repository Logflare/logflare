import css from "../css/app.scss"
import socket from "./socket"
import { Socket } from "phoenix"
import * as _ from "lodash"
import "bootstrap"
import "@babel/polyfill"
import $ from "jquery"
import ClipboardJS from "clipboard"
import * as Dashboard from "./dashboard"
import * as Source from "./source"
import * as Logs from "./logs"
import { LogSparklines } from "./source_log_chart.jsx"
import LiveReact, { initLiveReact } from "phoenix_live_react"

let liveReactHooks = { LiveReact }

document.addEventListener("DOMContentLoaded", e => {
  initLiveReact()
})

window.Components = { LogSparklines }

import LiveSocket from "phoenix_live_view"

import sourceLiveViewHooks from "./source_lv_hooks"

const hooks = Object.assign(liveReactHooks, sourceLiveViewHooks)

let liveSocket = new LiveSocket("/live", Socket, { hooks })
liveSocket.connect()

window.window.Dashboard = Dashboard
window.Logs = Logs
window.Source = Source
window.$ = $
window.ClipboardJS = ClipboardJS

