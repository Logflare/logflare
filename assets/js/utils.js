import ClipboardJS from "clipboard"

export function activateClipboardForSelector(selector, options) {
  const clipboard = new ClipboardJS(selector, options)

  clipboard.on("success", (e) => {
    const tooltip =
      document.getElementById("copy-tooltip") ||
      document.querySelector(".tooltip-inner")

    if (tooltip) {
      tooltip.innerHTML = "Copied!"
    }

    e.clearSelection()
  })
  clipboard.on("error", (e) => {
    console.log("Clipboard error!")
    e.clearSelection()
  })
}
