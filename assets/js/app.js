import "../css/app.scss";
import "phoenix"
import "phoenix_html"
import { Socket } from "phoenix";
import "../css/tailwind.css";

import "bootstrap";
import ClipboardJS from "clipboard";
import * as Dashboard from "./dashboard";
import * as Logs from "./logs";
import * as User from "./user";
import BillingHooks from "./billing";
import LiveModalHooks from "./live_modal";
import { LogEventsChart } from "./LogEventsChart.jsx";
import Chart from "./admin_dashboard_charts.jsx";
import Loader from "./loader.jsx";
import { LiveSocket } from "phoenix_live_view";
import LiveReact, { initLiveReact } from "phoenix_live_react";

import sourceLiveViewHooks from "./source_lv_hooks";
import logsLiveViewHooks from "./log_event_live_hooks";
import $ from "jquery";
import moment from "moment";
import { CodeEditorHook } from "../../deps/live_monaco_editor/priv/static/live_monaco_editor.esm"


// set moment globally before daterangepicker
window.moment = moment;

// import vendor files
import "./vendor/daterangepicker.min.js";
import "./vendor/daterangepicker.css";

let csrfToken = document
  .querySelector("meta[name='csrf-token']")
  .getAttribute("content");

const liveReactHooks = { LiveReact };

window.Components = { LogEventsChart, Loader, AdminChart: Chart };
window.Dashboard = Dashboard;
window.Logs = Logs;
window.User = User;
window.ClipboardJS = ClipboardJS;

const hooks = {
  ...liveReactHooks,
  ...sourceLiveViewHooks,
  ...logsLiveViewHooks,
  ...LiveModalHooks,
  ...BillingHooks,
  CodeEditorHook
  
};

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
      };
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
        shiftKey: e.shiftKey,
      };
    },
  },
});
liveSocket.enableDebug()

liveSocket.connect();
window.initLiveReact = initLiveReact;
window.liveSocket = liveSocket;

document.addEventListener("DOMContentLoaded", (e) => {
  initLiveReact();
});

// Use `:text` on the `:detail` optoin to pass values to event listener
window.addEventListener("logflare:copy-to-clipboard", (event) => {
  if ("clipboard" in navigator) {
    const text = event.detail?.text || event.target.textContent;
    const tooltip = document.getElementById("copy-tooltip") || document.querySelector(".tooltip-inner")
    if (tooltip) {
      tooltip.innerHTML = "Copied!";
    }
    if (event.target.textContent.trim() === "copy") {
      event.target.textContent = "copied";
      setTimeout(() => {
        event.target.textContent = "copy";
      }, 6000);
    }
    navigator.clipboard.writeText(text);
  } else {
    console.error("Your browser does not support clipboard copy.");
  }
});

window.addEventListener("phx:page-loading-stop", (_info) => {
  // enable all tooltips
  $(function () {
    $('[data-toggle="tooltip"]').tooltip({ delay: { show: 100, hide: 200 } });
  });
});
