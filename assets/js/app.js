import css from "../css/app.scss"
import "phoenix"
import "phoenix_html"
import socket from "./socket"
import "bootstrap"
import "@babel/polyfill"
import $ from "jquery"
import * as Dashboard from "./dashboard"
import * as Source from "./source"
import * as Logs from "./logs"
import * as formatters from "./formatters"

import LiveSocket from "phoenix_live_view"

let liveSocket = new LiveSocket("/live")
liveSocket.connect()


window.Dashboard = Dashboard
window.Logs = Logs
window.Source = Source
window.$ = $
