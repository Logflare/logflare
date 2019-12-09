import { activateClipboardForSelector } from "./utils"
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

hooks.SourceLogsSearchList = {
    updated() {
        window.scrollTo(0, document.body.scrollHeight)
    },
    mounted() {
        $("html, body").animate({ scrollTop: document.body.scrollHeight })
    },
}

hooks.SourceQueryDebugEventsModal = {
    mounted() {
        const $queryDebugModal = $(this.el)
        const code = $("#search-query-debug-events code")
        const fmtSql = sqlFormatter.format(code.text())
        // replace with formatted sql
        code.text(fmtSql)

        $queryDebugModal
            .find(".modal-body")
            .html($("#search-query-debug-events").html())
    },
}

hooks.SourceQueryDebugAggregatesModal = {
    mounted() {
        const $queryDebugModal = $(this.el)
        const code = $("#search-query-debug-aggregates code")
        const fmtSql = sqlFormatter.format(code.text())
        // replace with formatted sql
        code.text(fmtSql)

        $queryDebugModal
            .find(".modal-body")
            .html($("#search-query-debug-aggregates").html())
    },
}

hooks.SourceQueryDebugErrorModal = {
    mounted() {
        const $queryDebugModal = $(this.el)
        const code = $("#search-query-debug-error code")
        const fmtSql = sqlFormatter.format(code.text())
        // replace with formatted sql
        code.text(fmtSql)

        $queryDebugModal
          .find(".modal-body")
          .html($("#search-query-debug-error").html())
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
                const elapsed =
                    new Date().getTime() / 1000 - lastQueryCompletedAt
                $("#last-query-completed-at span").text(elapsed.toFixed(1))
            }
        }, 250)

        $("button#search").click()
    },
}

export default hooks
