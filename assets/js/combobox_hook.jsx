import { createRoot } from "react-dom/client";

import { Combobox } from "./Combobox.jsx";

const restoreLabels = (selectId, inputId, ownedLabels) => {
  for (const label of ownedLabels) {
    if (label.htmlFor === inputId) label.htmlFor = selectId;
  }

  ownedLabels.clear();
};

export default {
  Combobox: {
    mounted() {
      this.root = createRoot(this.el.querySelector("[data-combobox-container]"));
      this.form = this.el.closest("form");
      this.ownedLabels = new Set();
      this.onReset = () => requestAnimationFrame(() => this.renderCombobox());
      this.onInputMount = (input) => {
        this.input = input;
        if (input) this.syncLabelAssociations();
      };
      this.form?.addEventListener("reset", this.onReset);
      this.labelObserver = new MutationObserver(() => this.syncLabelAssociations());
      this.labelObserver.observe(this.form || this.el.parentElement, {
        attributeFilter: ["for"],
        attributes: true,
        childList: true,
        subtree: true,
      });
      this.renderCombobox();
    },

    updated() {
      this.renderCombobox();
      this.syncLabelAssociations();
    },

    destroyed() {
      this.form?.removeEventListener("reset", this.onReset);
      this.labelObserver.disconnect();
      restoreLabels(this.selectId, this.el.dataset.comboboxInputId, this.ownedLabels);
      this.root.unmount();
    },

    syncLabelAssociations() {
      if (!this.input) return;

      const select = this.el.querySelector("select");
      const inputId = this.el.dataset.comboboxInputId;
      this.selectId = select.id;

      for (const label of this.ownedLabels) {
        if (label.htmlFor !== inputId) this.ownedLabels.delete(label);
      }

      for (const label of Array.from(select.labels)) {
        if (label.htmlFor === select.id) {
          label.htmlFor = inputId;
          this.ownedLabels.add(label);
        }
      }
    },

    renderCombobox() {
      this.root.render(
        <Combobox
          emptyText={this.el.dataset.comboboxEmptyText}
          inputId={this.el.dataset.comboboxInputId}
          onInputMount={this.onInputMount}
          prompt={this.el.dataset.comboboxPrompt}
          select={this.el.querySelector("select")}
        />,
      );
    },
  },
};
