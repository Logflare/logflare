import { activateClipboardForSelector } from "./utils"

let hooks = {}

hooks.SourceSchemaModalTable = {
    mounted() {
        activateClipboardForSelector(
            `.${this.el.classList} .copy-metadata-field`
        )
    },
}

export default hooks
