import css from "../css/app.scss"
import socket from "./socket"
import { Socket } from "phoenix"
import "bootstrap"
import "@babel/polyfill"
import $ from "jquery"
import ClipboardJS from "clipboard"
import * as Dashboard from "./dashboard"
import * as Source from "./source"
import * as Logs from "./logs"

import LiveSocket from "phoenix_live_view"

import sourceLiveViewHooks from "./source_lv_hooks"

const hooks = Object.assign({}, sourceLiveViewHooks)

let liveSocket = new LiveSocket("/live", { hooks })
liveSocket.connect()

window.Dashboard = Dashboard
window.Logs = Logs
window.Source = Source
window.$ = $
window.ClipboardJS = ClipboardJS
