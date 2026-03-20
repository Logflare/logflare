import socket from "./socket"
import $ from "jquery"
import * as userConfig from "./user-config-storage"
import _ from "lodash"
import {
  userSelectedFormatter
} from "./formatters"
import {
  activateClipboardForSelector
} from "./utils"
import {
  applyToAllLogTimestamps
} from "./logs"
import { escape, get, isEmpty } from "lodash"

export async function main({
  scrollTracker
}, {
  avgEventsPerSecond
}) {
  const {
    sourceToken,
    logs
  } = $("#__phx-assigns__").data()
  await initLogsUiFunctions({
    scrollTracker
  })

  await initClipboards()

  if (avgEventsPerSecond < 25) {
    joinSourceChannel(sourceToken)
  }

  if (window.location.href.indexOf('new=true') > 0) {
    $("#sourceHelpModal").modal()
  } else {
    scrollBottom()
  }
}

async function initClipboards() {
  activateClipboardForSelector("#source-id", {
    container: document.getElementById('sourceHelpModal')
  })
}

export async function initLogsUiFunctions({
  scrollTracker
}) {
  await trackScroll({
    scrollTracker
  })

  await applyToAllLogTimestamps(await userSelectedFormatter())
  $("#logs-list").removeAttr("hidden")
}

export async function trackScroll({
  scrollTracker
}) {
  window.scrollTracker = scrollTracker

  window.addEventListener("scroll", () => {
    resetScrollTracker()
    swapDownArrow()
  })
}

const joinSourceChannel = (sourceToken) => {
  let channel = socket.channel(`source:${sourceToken}`, {})

  channel
    .join()
    .receive("ok", (resp) => {
      console.log(`Source ${sourceToken} channel joined successfully`, resp)
    })
    .receive("error", (resp) => {
      console.log(`Unable to join ${sourceToken} channel`, resp)
    })

  channel.on(`source:${sourceToken}:new`, renderLog)
}

async function renderLog(event) {
  const renderedLog = await logTemplate(event)

  $("#no-logs-warning").html("")
  $("#logs-list").append(renderedLog)

  if (window.scrollTracker) {
    scrollBottom()
  }
}

export function scrollBottom() {

  const y = document.body.clientHeight

  window.scrollTo(0, y)
}

async function logTemplate(e) {
  const { via_rule_id, source_uuid, body } = e;
  const metadata = JSON.stringify(body, null, 2);
  const formatter = await userSelectedFormatter();
  const formattedDatetime = formatter(body.timestamp);
  const randomId = Math.random() * 10e16;
  const metadataId = `metadata-${body.timestamp}-${randomId}`;
  const log_level = _.get(body, ["metadata", "level"]);

  const logLevelTemplate = log_level
    ? `<mark class="log-level-${log_level}">${log_level}</mark>`
    : "<span class='tw-w-[0.1rem] tw-inline-block'></span>";

  const metadataElement = !isEmpty(body?.metadata)
    ? `
    <a class="metadata-link" data-toggle="collapse" href="#${metadataId}" aria-expanded="false">
        event body
    </a>
    <div class="collapse metadata" id="${metadataId}">
        <pre class="pre-metadata"><code>${escape(metadata)}</code></pre>
    </div> ` :
    ""

  return `<li class="hover:tw-bg-gray-800">
    <mark class="log-datestamp" data-timestamp="${
      body.timestamp
    }">${formattedDatetime}</mark> ${logLevelTemplate}
    ${escape(body.event_message)}
    ${metadataElement}
    ${via_rule_id
      ? `<span
    data-toggle="tooltip" data-placement="top" title="Routed from ${source_uuid}" style="color: #5eeb8f;">
    <i class="fa fa-code-branch" style="font-size: 1em;"></i>
    </span>`
      : `<span></span>`
    }
</li>`
}

function swapDownArrow() {
  const scrollDownElem = $("#scroll-down")
  if (window.scrollTracker) {
    scrollDownElem.html(`<i class="fas fa-arrow-alt-circle-down"></i>`)
  } else {
    scrollDownElem.html(`<i class="far fa-arrow-alt-circle-down"></i>`)
  }
}

export async function switchDateFormat() {
  await userConfig.flipUseLocalTime()
  $("#swap-date > i").toggleClass("fa-toggle-off").toggleClass("fa-toggle-on")
  const formatter = await userSelectedFormatter()
  await applyToAllLogTimestamps(formatter)
}

function resetScrollTracker() {
  const observer =
      new IntersectionObserver((entries, observer) => {
        entries.forEach((entry) => {
          let searchInView = entry.isIntersecting
          if (searchInView) {
            // stick to bottom

            window.scrollTracker = true
          } else {
            // don't stick to bottom
            window.scrollTracker = false
          }
        })
      })

    const target = document.querySelector("#observer-target")
    observer.observe(target)
}

export function scrollOverflowBottom() {
  const $lastLog = $("#logs-list li:nth(0)")[0]
  if ($lastLog) {
    $lastLog.scrollIntoView()
  }
}
window.Source = {
  main,
  initLogsUiFunctions,
  trackScroll,
  scrollBottom,
  scrollOverflowBottom,
  switchDateFormat
}
