import $ from "jquery"
import socket from "./socket"
import { activateClipboardForSelector } from "./utils"
import { timestampNsToAgo } from "./formatters"
import { applyToAllLogTimestamps } from "./logs"

export async function main() {
  const { sourceTokens, apiKey, currentNode } = $("#__phx-assigns__").data()

  await initClipboards()
  await initApiClipboard()
  await initTooltips()

  for (let token of sourceTokens) {
    joinSourceChannel(token, currentNode)
  }
  await applyToAllLogTimestamps(timestampNsToAgo)

  $(".dashboard.container").removeAttr("hidden")
}

async function initTooltips() {
  $(".source-info").tooltip({ delay: { show: 100, hide: 200 } })
}

async function initClipboards() {
  activateClipboardForSelector(".copy-token")
}

async function initApiClipboard() {
  activateClipboardForSelector("#api-key")
}

function joinSourceChannel(sourceToken, currentNode) {
  let channel = socket.channel(`dashboard:${sourceToken}`, {})

  channel
    .join()
    .receive("ok", resp => {
      console.log(
        `Dashboard channel for source ${sourceToken} joined successfully on node ${currentNode}`,
        resp
      )
    })
    .receive("error", resp => {
      console.log("Unable to join", resp)
    })

  const sourceSelector = `#${sourceToken}`

  channel.on(`log_count`, event => {
    $(`${sourceSelector}-latest`).html(
      timestampNsToAgo(new Date().getTime() * 1000)
    )
    $(sourceSelector).html(
      `<small class="my-badge my-badge-info fade-in">${event.log_count}</small>`
    )
  })

  channel.on(`rate`, event => {
    $(`${sourceSelector}-rate`).html(`${event.rate}`)
    $(`${sourceSelector}-avg-rate`).html(`${event.average_rate}`)
    $(`${sourceSelector}-max-rate`).html(`${event.max_rate}`)
  })
  channel.on(`buffer`, event => {
    $(`${sourceSelector}-buffer`).html(`${event.buffer}`)
  })
}

export function showApiKey(apiKey) {
  const apiKeyElem = $("#api-key")
  let showingApiKey = apiKeyElem.data("showingApiKey")
  apiKeyElem.data("showingApiKey", !showingApiKey)
  showingApiKey = apiKeyElem.data("showingApiKey")
  apiKeyElem.text(showingApiKey ? apiKey : `CLICK ME`)
}
