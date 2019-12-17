import {activateClipboardForSelector} from "./utils"
import sqlFormatter from "sql-formatter"

import idle from "./vendor/idle"

let hooks = {}

hooks.SourceSchemaModalTable = {
    mounted() {
        activateClipboardForSelector(
            `.${this.el.classList} .copy-metadata-field`
        )
    },
}

let sourceLogsSearchListLastUpdate

const activateModal = (el, selector) => {
    const $modal = $(el)
    const code = $(`${selector} code`)
    const fmtSql = sqlFormatter.format(code.text())
    // replace with formatted sql
    code.text(fmtSql)

    $modal
        .find(".modal-body")
        .html($(selector).html())
}

hooks.SourceLogsSearchList = {
    updated() {
        let currentUpdate = $(this.el).attr("data-last-query-completed-at")
        if (sourceLogsSearchListLastUpdate !== currentUpdate) {
            sourceLogsSearchListLastUpdate = currentUpdate
            window.scrollTo(0, document.body.scrollHeight)
        }
    },
    mounted() {
        console.log("mounted called")
        $("html, body").animate({scrollTop: document.body.scrollHeight})
    },
}

hooks.SourceQueryDebugEventsModal = {
    updated() {
        activateModal(this.el, "#search-query-debug-events")
    },
    mounted() {
        activateModal(this.el, "#search-query-debug-events")
    },
}

hooks.SourceQueryDebugAggregatesModal = {
    updated() {
        activateModal(this.el, "#search-query-debug-aggregates")
    },

    mounted() {
        activateModal(this.el, "#search-query-debug-aggregates")
    },
}

hooks.SourceQueryDebugErrorModal = {
    updated() {
        activateModal(this.el, "#search-query-debug-error")
    },
    mounted() {
        activateModal(this.el, "#search-query-debug-error")
    },
}

hooks.SourceLogsSearch = {
    updated() {
        const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone
        $("#set_local_time").attr("phx-value-user_local_timezone", timeZone)
    },

    mounted() {
        activateClipboardForSelector("#search-uri-query", {
            text: () => location.href,
        })

        const idleInterval = $("#user-idle").data("user-idle-interval")

        // Set user timezone
        const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone
        $("#user-local-timezone").val(timeZone)

        // Activate user idle tracking
        idle({
            onIdle: () => {
                const $searchTailingButton = $("#search-tailing-button")
                const $searchTailingCheckbox = $(
                    "input#" + $.escapeSelector("search_tailing?")
                )

                if ($searchTailingCheckbox.prop("value") === "true") {
                    console.log(
                        `User idle for ${idleInterval}, tail search paused`
                    )
                    $searchTailingButton.click()
                    $("#user-idle").click()
                }
            },
            keepTracking: true,
            idle: idleInterval,
        }).start()

        setInterval(() => {
            const $lastQueryCompletedAt = $("#last-query-completed-at")
            const lastQueryCompletedAt = $lastQueryCompletedAt.attr(
                "data-timestamp"
            )
            if (lastQueryCompletedAt) {
                const elapsed = new Date().getTime() / 1000 - lastQueryCompletedAt
                this.pushEvent("set_last_query_elapsed_sec", elapsed)
            }
        }, 500)

        $("button#search").click()
    },
}

export default hooks
