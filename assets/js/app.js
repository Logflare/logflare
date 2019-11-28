import css from "../css/app.scss"
import socket from "./socket"
import { Socket } from "phoenix"
import "@babel/polyfill"
import * as _ from "lodash"
import $ from "jquery"
global.jQuery = $
global.$ = $
import "bootstrap"
import ClipboardJS from "clipboard"
import * as Dashboard from "./dashboard"
import * as Source from "./source"
import * as Logs from "./logs"
import { LogEventsChart } from "./source_log_chart.jsx"
import LiveSocket from "phoenix_live_view"
import LiveReact, { initLiveReact } from "phoenix_live_react"
import sourceLiveViewHooks from "./source_lv_hooks"

const liveReactHooks = { LiveReact }

window.Components = { LogEventsChart }
window.Dashboard = Dashboard
window.Logs = Logs
window.Source = Source
window.ClipboardJS = ClipboardJS

document.addEventListener("DOMContentLoaded", e => {
    initLiveReact()
})

const hooks = Object.assign(liveReactHooks, sourceLiveViewHooks)

let liveSocket = new LiveSocket("/live", Socket, { hooks })
liveSocket.connect()
