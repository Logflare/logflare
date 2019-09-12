import { activateClipboardForSelector } from "./utils"
import sqlFormatter from "sql-formatter"

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

hooks.SourceQueryDebugModal = {
    mounted() {
        const $queryDebugModal = $(this.el)
        const code = $("#search-query-debug code")
        const fmtSql = sqlFormatter.format(code.text())
        // replace with formatted sql
        code.text(fmtSql)

        $queryDebugModal
            .find(".modal-body")
            .html($("#search-query-debug").html())
    },
}
export default hooks
