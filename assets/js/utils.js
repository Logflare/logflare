import ClipboardJS from "clipboard"

export function activateClipboardForSelector(selector, options) {
    const clipboard = new ClipboardJS(selector, options)

    clipboard.on("success", e => {
        alert("Copied: " + e.text)
        e.clearSelection()
    })

    clipboard.on("error", e => {
        e.clearSelection()
    })
}
