import JSONFormatter from "json-formatter-js"
import $ from "jquery"

let hooks = {}

hooks.MetadataJsonViewer = {
  mounted() {
    $(".logflare-tooltip").tooltip({delay: {show: 100, hide: 200}})
    const json = JSON.parse(
      this.el.innerText.replace(/\\r/g, "\\\\r").replace(/\\n/g, "\\\\n")
    )

    const formatter = new JSONFormatter(json, Infinity, {theme: "logflare"})
    $("#metadata-viewer").html(formatter.render())
  },
}

export default hooks
