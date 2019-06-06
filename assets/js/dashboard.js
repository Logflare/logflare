import $ from "jquery"
import socket from "./socket"
import ClipboardJS from "clipboard"
import { timestampNsToAgo } from "./formatters"
import {applyToAllLogTimestamps} from "./logs"

export async function main() {
  const {sourceTokens, apiKey} = $("#__phx-assigns__").data()

  await initClipboards()
  await initApiClipboard()

  for (let token of sourceTokens) {
    joinSourceChannel(token)
  }
  await applyToAllLogTimestamps(timestampNsToAgo)

  $(".list-group").removeAttr("hidden")
}

async function initClipboards() {
  const clipboard = new ClipboardJS(".copy-token")

  clipboard.on("success", e => {
    alert("Copied: " + e.text)
    e.clearSelection()
  })

  clipboard.on("error", e => {
    e.clearSelection()
  })
}

async function initApiClipboard() {
  const clipboard = new ClipboardJS("#api-key")
  clipboard.on("success", e => {
    showApiKey(e.text)
    alert("Copied: " + e.text)
    e.clearSelection()
  })

  clipboard.on("error", e => {
    showApiKey()
    e.clearSelection()
  })
}

function joinSourceChannel(sourceToken) {
  let channel = socket.channel(`dashboard:${sourceToken}`, {})

  channel
    .join()
    .receive("ok", resp => {
      console.log(
        `Dashboard channel for source ${sourceToken} joined successfully`,
        resp,
      )
    })
    .receive("error", resp => {
      console.log("Unable to join", resp)
    })

  const sourceSelector = `#${sourceToken}`

  channel.on(`dashboard:${sourceToken}:log_count`, event => {
    $(`${sourceSelector}-latest`).html(timestampNsToAgo((new Date).getTime() * 1000))
    $(sourceSelector).html(`<small class="my-badge fade-in">${event.log_count}</small>`)
  })

  channel.on(`dashboard:${sourceToken}:rate`, event => {
    $(`${sourceSelector}-rate`).html(`${event.rate}`)
    $(`${sourceSelector}-avg-rate`).html(`${event.average_rate}`)
    $(`${sourceSelector}-max-rate`).html(`${event.max_rate}`)
  })
  channel.on(`dashboard:${sourceToken}:buffer`, event => {
    $(`${sourceSelector}-buffer`).html(`${event.buffer}`)
  })
}


function showApiKey(apiKey) {
  const apiKeyElem = $("#api-key")
  let showingApiKey = apiKeyElem.data("showingApiKey")
  apiKeyElem.data("showingApiKey", !showingApiKey)
  showingApiKey = apiKeyElem.data("showingApiKey")
  apiKeyElem.text(showingApiKey ? apiKey : `CLICK ME`)
}

