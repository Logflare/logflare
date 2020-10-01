import JSONFormatter from "json-formatter-js"
import {activateClipboardForSelector} from "./utils"
import $ from "jquery"

let hooks = {}

hooks.MetadataJsonViewer = {
  mounted() {
    $(".logflare-tooltip").tooltip({delay: {show: 100, hide: 200}})
    activateClipboardForSelector("#copy-metadata-raw", {
      text: () => {
        return $("#metadata-raw-json-code").text()
      },
    })

    activateClipboardForSelector("#log-event-uri", {
      text: () => location.href,
    })
    $("#log-event-uri").tooltip()

    const json = JSON.parse(
      this.el.innerText.replace(/\\r/g, "\\\\r").replace(/\\n/g, "\\\\n")
    )

    const formatter = new JSONFormatter(json, Infinity, {theme: "logflare"})
    document.getElementById("metadata-viewer").appendChild(formatter.render())
  },
}

export default hooks
