import { activateClipboardForSelector } from "./utils"
import sqlFormatter from "sql-formatter"
import $ from "jquery"

import idle from "./vendor/idle"

let hooks = {}

hooks.SourceSchemaModalTable = {
  mounted() {
    activateClipboardForSelector(`.${this.el.classList} .copy-metadata-field`)
  },
}

const activateModal = (el, selector) => {
  const $modal = $(el)
  const code = $(`${selector} code`)
  const fmtSql = sqlFormatter.format(code.text())
  // replace with formatted sql
  code.text(fmtSql)

  $modal.find(".modal-body").html($(selector).html())
}


const initSearchInViewObserver = (hook) => {
  const observer = new IntersectionObserver((entries, observer) => {
    entries.forEach(entry => {
      let searchInView = entry.isIntersecting
      if (searchInView) {
        hook.pushEvent("resume_live_search", {})
      }
      else {
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
    $("html, body").animate({ scrollTop: document.body.scrollHeight })
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

const setTimezone = () => {
  // Set user timezone
  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone
  $("#user-local-timezone").val(timeZone)
}

hooks.SourceLogsSearch = {
  updated() {
    setTimezone()
  },

  mounted() {
    initSearchInViewObserver(this)

    activateClipboardForSelector("#search-uri-query", {
      text: () => location.href,
    })

    setTimezone()

    // Activate user idle tracking
    const idleInterval = $("#user-idle").data("user-idle-interval")
    idle({
      onIdle: () => {
        const $searchTailingButton = $("#search-tailing-button")
        const $searchTailingCheckbox = $(
          "input#" + $.escapeSelector("search_tailing?")
        )

        if ($searchTailingCheckbox.prop("value") === "true") {
          console.log(`User idle for ${idleInterval}, tail search paused`)
          $searchTailingButton.click()
          $("#user-idle").click()
        }
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

    $("button#search").click()
  },
}

export default hooks
