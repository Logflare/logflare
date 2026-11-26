const QueryClassSettings = {
  mounted() {
    this.onInvalid = (event) => {
      const { target } = event;
      if (target !== this.el.querySelector("input:invalid")) return;

      const toggle = target
        .closest("[data-query-class]")
        ?.querySelector("button[aria-expanded='false']");

      if (toggle) {
        event.preventDefault();
        this.liveSocket.execJS(toggle, toggle.getAttribute("phx-click"));
        requestAnimationFrame(() => {
          target.focus();
          target.reportValidity();
        });
      }
    };

    this.el.addEventListener("invalid", this.onInvalid, true);
  },

  destroyed() {
    this.el.removeEventListener("invalid", this.onInvalid, true);
  },
};

export default QueryClassSettings;
