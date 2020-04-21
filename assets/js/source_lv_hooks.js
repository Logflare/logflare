import {activateClipboardForSelector} from "./utils"
import sqlFormatter from "sql-formatter"
import $ from "jquery"

import idle from "./vendor/idle"

let hooks = {}

hooks.SourceSchemaModalTable = {
  mounted() {
    activateClipboardForSelector(`.${this.el.classList} .copy-metadata-field`, {
      container: document.getElementById("logflare-modal"),
    })
    $(".copy-metadata-field").tooltip()
  },
}
const initSearchInViewObserver = (hook) => {
  const observer = new IntersectionObserver((entries, observer) => {
    entries.forEach((entry) => {
      let searchInView = entry.isIntersecting
      if (searchInView) {
        hook.pushEvent("resume_live_search", {})
      } else {
        hook.pushEvent("pause_live_search", {})
      }
    })
  })

  const target = document.querySelector("#search")
  observer.observe(target)
}

let sourceLogsSearchListLastUpdate
hooks.SourceLogsSearchList = {
  updated() {
    let currentUpdate = $(this.el).attr("data-last-query-completed-at")
    if (sourceLogsSearchListLastUpdate !== currentUpdate) {
      sourceLogsSearchListLastUpdate = currentUpdate
      window.scrollTo(0, document.body.scrollHeight)
    }
  },
  mounted() {
    $("html, body").animate({scrollTop: document.body.scrollHeight})
  },
}

const formatModal = ($modal) => {
  if ($modal.data("modal-type") === "search-op-debug-modal") {
    const $code = $modal.find(`code#search-op-sql-string`)
    const fmtSql = sqlFormatter.format($code.text())
    // replace with formatted sql
    $code.text(fmtSql)
    $modal.find("pre code").each((i, block) => {
      hljs.highlightBlock(block)
    })
  }
}

hooks.ModalHook = {
  updated() {
    const $modal = $(this.el)
    formatModal($modal)
  },
  mounted() {
    const $modal = $(this.el)

    if ($modal.data("modal-type") === "metadata-modal") {
      this.pushEvent("pause_live_search", {})
    }

    $modal.on("hidePrevented.bs.modal", () => {
      this.pushEvent("deactivate_modal", {})

      if ($modal.data("modal-type") === "metadata-modal") {
        this.pushEvent("resume_live_search", {})
      }
    })

    formatModal($modal)

    $modal.modal({backdrop: "static"})
  },
  destroyed() {
    const $body = $("body")
    $(".modal").modal("dispose")
    $body.removeClass("modal-open")
    $body.removeAttr("style")
    $(".modal-backdrop").remove()
  },
}

const datepickerConfig = {
  showDropdowns: true,
  timePicker: true,
  timePicker24Hour: true,
  drops: "up",
  alwaysShowCalendars: true,
  ranges: {
    Today: [],
    Yesterday: [],
    "Last 15 Minutes": [],
    "Last 7 Days": [],
    "This Month": [],
    "Last Month": [],
  },
}

const buildTsClause = (start, end, label) => {
  let timestampFilter
  const formatWithISO8601 = (x) => x.format("YYYY-MM-DDTHH:mm:ss")
  switch (label) {
    case "Today":
      timestampFilter = ["t:today"]
      break
    case "Yesterday":
      timestampFilter = ["t:yesterday"]
      break
    case "Last 15 Minutes":
      timestampFilter = ["t:last@15m"]
      break
    case "Last 7 Days":
      timestampFilter = ["t:last@7d"]
      break
    case "This Month":
      timestampFilter = ["t:this@month"]
      break
    case "Last Month":
      timestampFilter = ["t:last@month"]
      break
    default:
      timestampFilter = [
        `t:${formatWithISO8601(start)}..${formatWithISO8601(end)}`,
      ]
      break
  }
  return timestampFilter.join(" ")
}

hooks.SourceLogsSearch = {
  updated() {
    const hook = this
    const $daterangepicker = $("#daterangepicker")
    $daterangepicker.daterangepicker(datepickerConfig)
    $daterangepicker.on("apply.daterangepicker", (e, picker) => {
      const tsClause = buildTsClause(
        picker.startDate,
        picker.endDate,
        picker.chosenLabel
      )
      hook.pushEvent("timestamp_and_chart_update", {querystring: tsClause})
    })

    activateClipboardForSelector("#search-uri-query", {
      text: () => location.href,
    })
    $("#search-uri-query").tooltip()

    $daterangepicker.on("show.daterangepicker", (e, picker) => {
      hook.pushEvent("stop_live_search", {})
    })
  },
  reconnected() {},
  mounted() {
    const hook = this
    const $daterangepicker = $("#daterangepicker")
    $daterangepicker.daterangepicker(datepickerConfig)
    $daterangepicker.on("apply.daterangepicker", (e, picker) => {
      const tsClause = buildTsClause(
        picker.startDate,
        picker.endDate,
        picker.chosenLabel
      )
      hook.pushEvent("timestamp_and_chart_update", {querystring: tsClause})
    })

    window.stopLiveSearch = () => hook.pushEvent("stop_live_search", {})

    window.updateTimestampAndChart = (tsClause, chartPeriod) => {
      hook.pushEvent("timestamp_and_chart_update", {
        querystring: tsClause,
        period: chartPeriod,
      })
    }

    $daterangepicker.on("show.daterangepicker", (e, picker) => {
      hook.pushEvent("stop_live_search", {})
    })

    initSearchInViewObserver(this)

    activateClipboardForSelector("#search-uri-query", {
      text: () => location.href,
    })
    $("#search-uri-query").tooltip()

    // Activate user idle tracking
    const idleInterval = $("#user-idle").data("user-idle-interval")
    idle({
      onIdle: () => {
        hook.pushEvent("stop_live_search", {})
        console.log(
          `User idle for ${idleInterval}, live tail search stopped...`
        )
      },
      keepTracking: true,
      idle: idleInterval,
    }).start()

    setInterval(() => {
      const $lastQueryCompletedAt = $("#last-query-completed-at")
      const lastQueryCompletedAt = $lastQueryCompletedAt.attr("data-timestamp")
      if (lastQueryCompletedAt) {
        const elapsed = new Date().getTime() / 1000 - lastQueryCompletedAt
        $("#last-query-completed-at span").text(elapsed.toFixed(1))
      }
    }, 250)
  },
}

export default hooks
