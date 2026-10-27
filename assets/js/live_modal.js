import $ from "jquery"
import { activateDelegatedTooltips } from "./utils"

export default {
  LiveModal: {
    updated() {
      // Dispose tooltips whose trigger elements were removed by this update.
      $(".tooltip").each((_, el) => {
        const tooltip = $(el).data("bs.tooltip")
        if (tooltip && !tooltip.element.isConnected) tooltip.dispose()
      })
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
      $(this.el).find('[data-toggle="tooltip"]').tooltip("dispose")
      $(this.el).tooltip("dispose")
      $("body").removeClass("modal-open").removeAttr("style")
      $(".modal-backdrop").remove()
    },
  },
}
