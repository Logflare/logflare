import socket from "./socket"
import $ from "jquery"
import * as userConfig from "./user-config-storage"
import _ from "lodash"
import { userSelectedFormatter } from "./formatters"
import { activateClipboardForSelector } from "./utils"
import { applyToAllLogTimestamps } from "./logs"
import idle from "./vendor/idle"
import sqlFormatter from "sql-formatter"

export async function main({ scrollTracker }, { avgEventsPerSecond }) {
    const { sourceToken, logs } = $("#__phx-assigns__").data()
    await initLogsUiFunctions({ scrollTracker })

    if (avgEventsPerSecond < 25) {
        joinSourceChannel(sourceToken)
    }

    if (logs.length === 0) {
        $("#sourceHelpModal").modal()
    } else {
        scrollBottom()
    }
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

const joinSourceChannel = sourceToken => {
    let channel = socket.channel(`source:${sourceToken}`, {})

    channel
        .join()
        .receive("ok", resp => {
            console.log(
                `Source ${sourceToken} channel joined successfully`,
                resp
            )
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
        <pre class="pre-metadata"><code>${metadata}</code></pre>
    </div> `
        : ""

    return `<li>
    <mark class="log-datestamp" data-timestamp="${
        body.timestamp
    }">${formattedDatetime}</mark> ${body.message}
    ${metadataElement}
    ${
        via_rule
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
    $("#swap-date > i")
        .toggleClass("fa-toggle-off")
        .toggleClass("fa-toggle-on")
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
    } else {
        window.scrollTracker = false
    }
}

export async function initSearch() {
    // Clipboards
    activateClipboardForSelector("#search-uri-query", {
        text: trigger =>
            location.href.replace(/\?.+$/, "") +
            trigger.getAttribute("data-clipboard-text"),
    })

    activateClipboardForSelector(".show-source-schema td.metadata-field")

    const idleInterval = $("#user-idle").data("user-idle-interval")

    // Activate user idle tracking
    idle({
        onIdle: () => {
            console.log(`User idle for ${idleInterval}, tail search paused`)
            const $search_tailing = $(
                "#search-tailing-button #" + $.escapeSelector("search_tailing?")
            )

            if ($search_tailing.prop("value") === "true") {
                $search_tailing.click()
            }
        },
        keepTracking: true,
        idle: idleInterval,
    }).start()

    document.addEventListener("phx:update", search)
}

export async function search() {
    // Configure modals interactions
    const metadataModal = $("#metadataModal")
    metadataModal.on("show.bs.modal", event => {
        const metadataHtml = $(event.relatedTarget)
            .find("~ .metadata")
            .html()
        const modalBody = metadataModal.find(".modal-body")

        modalBody.html(metadataHtml)
    })

    const queryDebugModal = $("#queryDebugModal")
    queryDebugModal.on("show.bs.modal", event => {
        const code = $("#search-query-debug code")
        const fmtSql = sqlFormatter.format(code.text())
        // replace with formatted sql
        code.text(fmtSql)

        queryDebugModal
            .find(".modal-body")
            .html($("#search-query-debug").html())
    })
}
