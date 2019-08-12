import css from "../css/app.scss"
import socket from "./socket"
import "bootstrap"
import "@babel/polyfill"
import $ from "jquery"
import ClipboardJS from "clipboard"
import * as Dashboard from "./dashboard"
import * as Source from "./source"
import * as Logs from "./logs"

import LiveSocket from "phoenix_live_view"

let liveSocket = new LiveSocket("/live")
liveSocket.connect()

window.Dashboard = Dashboard
window.Logs = Logs
window.Source = Source
window.$ = $
window.ClipboardJS = ClipboardJS
