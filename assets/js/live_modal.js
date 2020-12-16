export default {
  LiveModal: {
    updated() {},
    mounted() {
      const $modal = $(this.el)

      $modal.modal({backdrop: "static"})
    },
    destroyed() {
      $("body").removeClass("modal-open").removeAttr("style")
      $(".modal-backdrop").remove()
    },
  },
}
