import {
  activateClipboardForSelector
} from "./utils"
import $ from "jquery"
import _ from "lodash"
import idle from "./vendor/idle"
import hljs from "highlight.js"
import "highlight.js/styles/tomorrow-night-blue.css"
import { applyToAllLogTimestamps } from "./logs";
import { timestampNsToAgo } from "./formatters";

let hooks = {}

hooks.SourceSchemaModalTable = {
  mounted() {
    activateClipboardForSelector(`.${this.el.classList} .copy-metadata-field`, {
      container: document.getElementById("logflare-modal"),
    })
    $(".copy-metadata-field").tooltip()
  },
}

hooks.SourceLogsSearchList = {
  updated() {
    const hook = this

    window.scrollTo(0, document.body.scrollHeight)

    const observer =
      new IntersectionObserver((entries, observer) => {
        entries.forEach((entry) => {
          let searchInView = entry.isIntersecting
          if (searchInView) {
            // play when we scroll down
            // hook.pushEvent("soft_play", {})
          } else {
            // pause when we scroll up
            hook.pushEvent("soft_pause", {})
          }
        })
      })

    const target = document.querySelector("#observer-target")
    observer.observe(target)
  },
  mounted() {
    $("html, body").animate({
      scrollTop: document.body.scrollHeight
    })
  },
}

hooks.ModalHook = {
  mounted() {
    const $modal = $(this.el)

    if ($modal.data("modal-type") === "metadata-modal") {
      this.pushEvent("soft_pause", {})
    }

    $modal.on("hidePrevented.bs.modal", () => {
      this.pushEvent("deactivate_modal", {})

      if ($modal.data("modal-type") === "metadata-modal") {
        this.pushEvent("soft_play", {})
      }
    })

    $modal.modal({
      backdrop: "static"
    })
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

hooks.BigQuerySqlQueryFormatter = {
  mounted() {
    this.formatSql()
  },
  updated() {
    this.formatSql()
  },
  formatSql() {
    const $this = $(this.el)
    const $code = $this.find(`code#search-op-sql-string`)
    const fmtSql = sqlFormatter.format($code.text())
    // replace with formatted sql
    $code.text(fmtSql)
    $this.find("pre code").each((i, block) => {
      hljs.highlightBlock(block)
    })
  },
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
      hook.pushEvent("datetime_update", {
        querystring: tsClause
      })
    })


    $daterangepicker.on("cancel.daterangepicker", (e, picker) => {
      hook.pushEvent("soft_play", {})
    })

    activateClipboardForSelector("#search-uri-query", {
      text: () => location.href,
    })
    $("#search-uri-query").tooltip()

    $daterangepicker.on("show.daterangepicker", (e, picker) => {
      hook.pushEvent("soft_pause", {})
    })
  },
  reconnected() { },
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
      hook.pushEvent("datetime_update", {
        querystring: tsClause
      })
    })

    $daterangepicker.on("cancel.daterangepicker", (e, picker) => {
      hook.pushEvent("soft_play", {})
    })

    $daterangepicker.on("show.daterangepicker", (e, picker) => {
      hook.pushEvent("soft_pause", {})
    })

    activateClipboardForSelector("#search-uri-query", {
      text: () => location.href,
    })
    $("#search-uri-query").tooltip()

    // Activate user idle tracking
    const idleInterval = $("#user-idle").data("user-idle-interval")
    idle({
      onIdle: () => {
        hook.pushEvent("soft_pause", {})
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
};

/* Listens for a `scrollIntoView` event on the element

  Example:

  <div phx-mounted={JS.dispatch("scrollIntoView")}>
    ...
  </div>
*/
hooks.ScrollIntoView = {
  mounted() {
    this.el.addEventListener("scrollIntoView", (event) => {
      event.target.scrollIntoView({ behavior: "instant" });
    });
  },
};

hooks.FormatTimestamps = {
  mounted() {
    applyToAllLogTimestamps(timestampNsToAgo);
  },

  updated() {
    applyToAllLogTimestamps(timestampNsToAgo);
  },
};

hooks.DocumentVisibility = {
  mounted() {
    this.handleVisibilityChange = () => {
      this.pushEvent("visibility_change", {
        visibility: document.visibilityState,
      });
    };
    document.addEventListener("visibilitychange", this.handleVisibilityChange);
  },

  destroyed() {
    document.removeEventListener(
      "visibilitychange",
      this.handleVisibilityChange,
    );
  },
};

hooks.LiveTooltips = {
  mounted() {
    $(this.el).tooltip({
      selector: ".logflare-tooltip",
      delay: { show: 100, hide: 200 },
    });
  },
  destroyed() {
    $(this.el).tooltip("dispose");
  },
};

export default hooks;
