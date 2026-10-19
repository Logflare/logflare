import { createRoot } from "react-dom/client";

import { Combobox } from "./Combobox.jsx";

export default {
  Combobox: {
    mounted() {
      this.root = createRoot(this.el.querySelector("[data-combobox-container]"));
      this.renderCombobox();
    },

    updated() {
      this.renderCombobox();
    },

    destroyed() {
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
