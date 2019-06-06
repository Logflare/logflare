import socket from "./socket"
import $ from "jquery"
import * as userConfig from "./user-config-storage"
import { userSelectedFormatter } from "./formatters"
import { applyToAllLogTimestamps } from "./logs"

export async function main({ scrollTracker }) {
  window.scrollTracker = scrollTracker
  window.addEventListener("scroll", () => {
    resetScrollTracker()
    swapDownArrow()
  })

  const { sourceToken, logs } = $("#__phx-assigns__").data()

  joinSourceChannel(sourceToken)
  await applyToAllLogTimestamps(await userSelectedFormatter())


  $("#logs-list").removeAttr("hidden")

  if (logs.length === 0) {
    $("#sourceHelpModal").modal()
  }
  else {
    scrollBottom()
  }
}

const joinSourceChannel = (sourceToken) => {
  let channel = socket.channel(`source:${sourceToken}`, {})

  channel.join()
    .receive("ok", resp => {
      console.log(`Source ${sourceToken} channel joined successfully`, resp)
    })
    .receive("error", resp => {
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
  const metadata = JSON.stringify(e.metadata, null, 2)
  const formatter = await userSelectedFormatter()
  const formattedDatetime = formatter(e.timestamp)

  const metadataElement = e.metadata ? `
    <a class="metadata-link" data-toggle="collapse" href="#metadata-${e.timestamp}" aria-expanded="false">
        metadata
    </a>
    <div class="collapse metadata" id="metadata-${e.timestamp}">
        <pre class="pre-metadata"><code>${metadata}</code></pre>
    </div> ` : ""

  return `<li>
    <mark class="log-datestamp" data-timestamp="${e.timestamp}">${formattedDatetime}</mark> ${e.log_message} 
    ${metadataElement}
</li>`
}

function swapDownArrow() {
  const scrollDownElem = $("#scroll-down")
  if (window.scrollTracker) {
    scrollDownElem.html(`<i class="fas fa-arrow-alt-circle-down"></i>`)
  }
  else {
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

  if (window_inner_height + window_offset - nav_height === client_height) {
    window.scrollTracker = true
  }
  else {
    window.scrollTracker = false
  }
}
