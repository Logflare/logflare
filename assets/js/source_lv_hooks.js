import { activateClipboardForSelector } from "./utils"

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
export default hooks
