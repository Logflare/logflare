import "../css/app.scss"
import { Socket } from "phoenix"
import "@babel/polyfill"
import "bootstrap"
import ClipboardJS from "clipboard"
import * as Dashboard from "./dashboard"
import * as Source from "./source"
import * as Logs from "./logs"
import * as User from "./user"
import { LogEventsChart } from "./source_log_chart.jsx"
import Chart from "./admin_dashboard_charts.jsx"
import Loader from "./loader.jsx"
import LiveSocket from "phoenix_live_view"
import LiveReact, { initLiveReact } from "phoenix_live_react"

import sourceLiveViewHooks from "./source_lv_hooks"

let csrfToken = document
  .querySelector("meta[name='csrf-token']")
  .getAttribute("content")

const liveReactHooks = { LiveReact }

window.Components = { LogEventsChart, Loader, AdminChart: Chart }
window.Dashboard = Dashboard
window.Logs = Logs
window.Source = Source
window.User = User
window.ClipboardJS = ClipboardJS

const hooks = Object.assign(liveReactHooks, sourceLiveViewHooks)

let liveSocket = new LiveSocket("/live", Socket, {
  hooks,
  params: {
    _csrf_token: csrfToken,
    user_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  },
  metadata: {
    click: (e, el) => {
      return {
        altKey: e.altKey,
        shiftKey: e.shiftKey,
        ctrlKey: e.ctrlKey,
        metaKey: e.metaKey,
        x: e.x || e.clientX,
        y: e.y || e.clientY,
        pageX: e.pageX,
        pageY: e.pageY,
        screenX: e.screenX,
        screenY: e.screenY,
        offsetX: e.offsetX,
        offsetY: e.offsetY,
        detail: e.detail || 1,
      }
    },
    keydown: (e, el) => {
      return {
        altGraphKey: e.altGraphKey,
        altKey: e.altKey,
        code: e.code,
        ctrlKey: e.ctrlKey,
        key: e.key,
        keyIdentifier: e.keyIdentifier,
        keyLocation: e.keyLocation,
        location: e.location,
        metaKey: e.metaKey,
        repeat: e.repeat,
        shiftKey: e.shiftKey
      }
    }
  }
})


liveSocket.connect()

window.initLiveReact = initLiveReact
window.liveSocket = liveSocket

document.addEventListener("DOMContentLoaded", (e) => {
  initLiveReact()
})
