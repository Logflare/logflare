import $ from "jquery"
import {activateClipboardForSelector} from "./utils"

export const initEditPage = () => {
  activateClipboardForSelector(`#copy-path`)
  $("#copy-path").tooltip()
}


