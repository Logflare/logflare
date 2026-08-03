import $ from "jquery"
import { activateDelegatedTooltips } from "./utils"

export default {
  LiveModal: {
    updated() {
      activateDelegatedTooltips(this.el, '[data-toggle="tooltip"]')
    },
    mounted() {
      const $modal = $(this.el)

      $modal.modal({ backdrop: "static" }).on("hidePrevented.bs.modal", (e) => {
        // click outside modal so phx-click-away is triggered
        $("main").trigger("click");
      });

      activateDelegatedTooltips(this.el, '[data-toggle="tooltip"]')
    },
    destroyed() {
      $(this.el).tooltip("dispose")
      $("body").removeClass("modal-open").removeAttr("style")
      $(".modal-backdrop").remove()
    },
  },
}
