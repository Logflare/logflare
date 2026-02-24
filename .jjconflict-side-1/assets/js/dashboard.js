import $ from "jquery"
import {activateClipboardForSelector} from "./utils"
import {timestampNsToAgo} from "./formatters"
import {applyToAllLogTimestamps} from "./logs"

export async function main() {
  const {sourceTokens, apiKey, currentNode} = $("#__phx-assigns__").data()

  await initClipboards()
  await initApiClipboard()
  await initTooltips()

  await applyToAllLogTimestamps(timestampNsToAgo)

  $(".dashboard.container").removeAttr("hidden")
}

async function initTooltips() {
  $(".logflare-tooltip").tooltip({delay: {show: 100, hide: 200}})
}

async function initClipboards() {
  activateClipboardForSelector(".copy-token")
}

async function initApiClipboard() {
  activateClipboardForSelector("#api-key")
}


export function showApiKey(apiKey) {
  const apiKeyElem = $("#api-key")
  let showingApiKey = apiKeyElem.data("showingApiKey")
  apiKeyElem.data("showingApiKey", !showingApiKey)
  showingApiKey = apiKeyElem.data("showingApiKey")
  apiKeyElem.text(showingApiKey ? apiKey : `CLICK ME`)
}
