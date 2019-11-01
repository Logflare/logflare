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
    mounted() {
        $("#logs-list li:nth(1)")[0].scrollIntoView()
    },
}

hooks.SourceQueryDebugModal = {
    mounted() {
        const $queryDebugModal = $(this.el)
        const code = $("#search-query-debug code")
        const fmtSql = sqlFormatter.format(code.text())
        // replace with formatted sql
        code.text(fmtSql)

        $queryDebugModal
            .find(".modal-body")
            .html($("#search-query-debug").html())
    },
}

hooks.SourceLogsSearch = {
    mounted() {
        activateClipboardForSelector("#search-uri-query", {
            text: trigger =>
                location.href.replace(/\?.+$/, "") +
                trigger.getAttribute("data-clipboard-text"),
        })

        const idleInterval = $("#user-idle").data("user-idle-interval")
        const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone
        $("#search-local-time").attr("phx-value-local_time_timezone", timeZone)

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
        $("button#search").click()
    },
}
export default hooks
