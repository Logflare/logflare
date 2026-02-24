import $ from "jquery"
export default {
  LiveModal: {
    updated() {},
    mounted() {
      const $modal = $(this.el)

      $modal.modal({ backdrop: "static" }).on("hidePrevented.bs.modal", (e) => {
        // click outside modal so phx-click-away is triggered
        $("main").trigger("click");
      });
    },
    destroyed() {
      $("body").removeClass("modal-open").removeAttr("style")
      $(".modal-backdrop").remove()
    },
  },
}
