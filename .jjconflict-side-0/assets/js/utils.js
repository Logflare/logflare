import ClipboardJS from "clipboard"
import $ from "jquery"

export function activateDelegatedTooltips(el, selector) {
  $(el).tooltip({
    selector,
    delay: { show: 100, hide: 200 },
  })
}

export function activateClipboardForSelector(selector, options) {
  const clipboard = new ClipboardJS(selector, options)

  clipboard.on("success", (e) => {
    document.getElementById("copy-tooltip").innerHTML = "Copied!"
    e.clearSelection()
  })
  clipboard.on("error", (e) => {
    console.log("Clipboard error!")
    e.clearSelection()
  })
}
