import { createRoot } from "react-dom/client";

import { Combobox } from "./Combobox.jsx";

export default {
  Combobox: {
    mounted() {
      this.root = createRoot(this.el.querySelector("[data-combobox-container]"));
      this.form = this.el.closest("form");
      this.onReset = () => requestAnimationFrame(() => this.renderCombobox());
      this.form?.addEventListener("reset", this.onReset);
      this.renderCombobox();
    },

    updated() {
      this.renderCombobox();
    },

    destroyed() {
      this.form?.removeEventListener("reset", this.onReset);
      this.root.unmount();
    },

    renderCombobox() {
      this.root.render(
        <Combobox
          emptyText={this.el.dataset.comboboxEmptyText}
          prompt={this.el.dataset.comboboxPrompt}
          select={this.el.querySelector("select")}
        />,
      );
    },
  },
};
