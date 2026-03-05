import {
  registerLqlLanguage,
  registerLqlCompletionProvider,
} from "./lql_language";

const parseSchemaFields = (schemaFieldsJson) => {
  if (!schemaFieldsJson) return [];

  try {
    return JSON.parse(schemaFieldsJson);
  } catch {
    return [];
  }
};

const LqlEditorWrapper = {
  mounted() {
    this._schemaFields = parseSchemaFields(this.el.dataset.schemaFieldsJson);
    this._completionDisposable = null;
    this._editor = null;

    // Listen for CodeEditorHook's mount event (bubbles from inner element)
    this.el.addEventListener("lme:editor_mounted", (event) => {
      const { editor } = event.detail;
      const standaloneEditor = editor.standalone_code_editor;
      this._editor = standaloneEditor;

      const monaco = window.monaco;

      // Register LQL language and set model language
      registerLqlLanguage(monaco);
      const model = standaloneEditor.getModel();
      monaco.editor.setModelLanguage(model, "lql");

      // Register completion provider
      this._completionDisposable = registerLqlCompletionProvider(
        monaco,
        () => this._schemaFields,
      );

      // Enter submits the form (only when suggest widget is NOT visible)
      standaloneEditor.addCommand(
        monaco.KeyCode.Enter,
        () => {
          const form = this.el.closest("form");
          if (form) {
            form.dispatchEvent(
              new Event("submit", { bubbles: true, cancelable: true }),
            );
          }
        },
        "!suggestWidgetVisible",
      );

      // Sync value to hidden input on change
      standaloneEditor.onDidChangeModelContent(() => {
        const value = standaloneEditor.getValue();
        const hiddenInput = this.el.querySelector(
          'input[type="hidden"][name="search[querystring]"]',
        );
        if (hiddenInput) {
          hiddenInput.value = value;
          hiddenInput.dispatchEvent(new Event("input", { bubbles: true }));
        }
      });

      // Forward focus/blur to LiveView
      standaloneEditor.onDidFocusEditorText(() => {
        this.pushEvent("form_focus", { value: standaloneEditor.getValue() });
      });

      standaloneEditor.onDidBlurEditorText(() => {
        this.pushEvent("form_blur", { value: standaloneEditor.getValue() });
      });
    });
  },

  updated() {
    this._schemaFields = parseSchemaFields(this.el.dataset.schemaFieldsJson);
  },

  destroyed() {
    if (this._completionDisposable) {
      this._completionDisposable.dispose();
      this._completionDisposable = null;
    }
  },
};

export default LqlEditorWrapper;
