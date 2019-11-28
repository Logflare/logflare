import { activateClipboardForSelector } from "./utils"
import sqlFormatter from "sql-formatter"
import idle from "./vendor/idle"
import appear from "./vendor/appear"

let hooks = {}

hooks.SourceSchemaModalTable = {
  mounted() {
    activateClipboardForSelector(
      `.${this.el.classList} .copy-metadata-field`,
    )
  },
}

hooks.SourceLogsSearchList = {
  mounted() {
    $("#logs-list li:nth(1)")[0].scrollIntoView()
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

hooks.SourceLogsSearch = {
  updated() {
    const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone
    console.log("Setting timezone " + timeZone)
    $("#search-local-time").attr("phx-value-user_local_timezone", timeZone)
  },

  mounted() {
    activateClipboardForSelector("#search-uri-query", {
      text: trigger =>
        location.href.replace(/\?.+$/, "") +
        trigger.getAttribute("data-clipboard-text"),
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
          "input#" + $.escapeSelector("search_tailing?"),
        )

        if ($searchTailingCheckbox.prop("value") === "true") {
          console.log(
            `User idle for ${idleInterval}, tail search paused`,
          )
          $searchTailingButton.click()
          $("#user-idle").click()
        }
      },
      keepTracking: true,
      idle: idleInterval,
    }).start()

    appear({
      init: () => {
      },
      elements: () => $("button#search")
      ,
      appear: (el) => { },
      disappear: (el) => this.pushEvent("search_control_out_of_view", {}) ,
      bounds: 200,
      reappear: true,
    })

    setInterval(() => {
      const $lastQueryCompletedAt = $("#last-query-completed-at")
      const lastQueryCompletedAt = $lastQueryCompletedAt.attr("data-timestamp")
      if (lastQueryCompletedAt) {
        const elapsed = new Date().getTime()/1000 - lastQueryCompletedAt
        $("#last-query-completed-at span").text(elapsed.toFixed(1))
      }
    }, 250)

    $("button#search").click()
  },
}

export default hooks
