import socket from "./socket"
import $ from "jquery"
import * as userConfig from "./user-config-storage"
import _ from "lodash"
import { userSelectedFormatter } from "./formatters"
import { activateClipboardForSelector } from "./utils"
import { applyToAllLogTimestamps } from "./logs"

export async function main({ scrollTracker }, { avgEventsPerSecond }) {
  const { sourceToken, logs } = $("#__phx-assigns__").data()
  await initLogsUiFunctions({ scrollTracker })

  await initClipboards()
  await initTooltips()

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

async function initTooltips() {
  $(".logflare-tooltip").tooltip({ delay: { show: 100, hide: 200 } })
}

export async function initLogsUiFunctions({ scrollTracker }) {
  await trackScroll({ scrollTracker })

  await applyToAllLogTimestamps(await userSelectedFormatter())
  $("#logs-list").removeAttr("hidden")
}

export async function trackScroll({ scrollTracker }) {
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
  const { via_rule, origin_source_id, body } = e
  const metadata = JSON.stringify(body.metadata, null, 2)
  const formatter = await userSelectedFormatter()
  const formattedDatetime = formatter(body.timestamp)
  const randomId = Math.random() * 10e16
  const metadataId = `metadata-${body.timestamp}-${randomId}`

  const metadataElement = !_.isEmpty(body.metadata)
    ? `
    <a class="metadata-link" data-toggle="collapse" href="#${metadataId}" aria-expanded="false">
        metadata
    </a>
    <div class="collapse metadata" id="${metadataId}">
        <pre class="pre-metadata"><code> ${_.escape(metadata)}</code></pre>
    </div> `
    : ""

  return `<li>
    <mark class="log-datestamp" data-timestamp="${body.timestamp
    }">${formattedDatetime}</mark> ${_.escape(body.message)}
    ${metadataElement}
    ${via_rule
      ? `<span
    data-toggle="tooltip" data-placement="top" title="Matching ${via_rule.regex} routing from ${origin_source_id}" style="color: ##5eeb8f;">
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
  let window_inner_height = window.innerHeight
  let window_offset = window.pageYOffset
  let client_height = document.body.clientHeight
  // should make this dynamic
  let nav_height = 110

  // even if we're close to the bottom, we're at the bottom (for mobile browsers)
  if (window_inner_height + window_offset - nav_height >= client_height - 100) {
    window.scrollTracker = true
  } else {
    window.scrollTracker = false
  }
}

export function scrollOverflowBottom() {
  const $lastLog = $("#logs-list li:nth(0)")[0]
  if ($lastLog) {
    $lastLog.scrollIntoView()
  }
}
