import {
  hasLqlSuggestions,
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
    this._suggestedSearches = [];
    this._completionDisposable = null;
    this._editor = null;
    this._refreshSuggestionsTimer = null;
    this._skipNextSavedSearchRequest = false;

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
        () => this._suggestedSearches,
      );

      standaloneEditor.addAction({
        id: "lql.dismissSavedSearchSuggest",
        label: "Dismiss saved search suggest",
        run: () => {
          this._skipNextSavedSearchRequest = true;

          const suggestController = standaloneEditor.getContribution(
            "editor.contrib.suggestController",
          );

          suggestController?.cancelSuggestWidget?.();
        },
      });

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

        if (this._skipNextSavedSearchRequest) {
          this._skipNextSavedSearchRequest = false;
          clearTimeout(this._refreshSuggestionsTimer);
          return;
        }

        clearTimeout(this._refreshSuggestionsTimer);
        this._refreshSuggestionsTimer = window.setTimeout(() => {
          this.refreshSuggestions();
        }, 100);
      });

      // Forward focus/blur to LiveView
      standaloneEditor.onDidFocusEditorText(() => {
        this.pushEvent(
          "form_focus",
          { value: standaloneEditor.getValue() },
          ({ suggestions = [] }) => {
            this._suggestedSearches = Array.isArray(suggestions)
              ? suggestions
              : [];

            this.refreshSuggestions();
          },
        );
      });

      standaloneEditor.onDidBlurEditorText(() => {
        this.pushEvent("form_blur", { value: standaloneEditor.getValue() });
      });
    });
  },

  updated() {
    this._schemaFields = parseSchemaFields(this.el.dataset.schemaFieldsJson);
  },

  refreshSuggestions() {
    const model = this._editor?.getModel();
    const position = this._editor?.getPosition();
    const suggestController = this._editor?.getContribution?.(
      "editor.contrib.suggestController",
    );

    suggestController?.cancelSuggestWidget?.();

    if (!model || !position) {
      return;
    }

    const textUntilPosition = model.getValueInRange({
      startLineNumber: position.lineNumber,
      startColumn: 1,
      endLineNumber: position.lineNumber,
      endColumn: position.column,
    });
    const fullLine = model.getLineContent(position.lineNumber);

    if (
      !hasLqlSuggestions(
        textUntilPosition,
        fullLine,
        this._schemaFields,
        this._suggestedSearches,
      )
    ) {
      return;
    }

    if (suggestController?.triggerSuggest) {
      suggestController.triggerSuggest();
      return;
    }

    this._editor?.getAction("editor.action.triggerSuggest").run();
  },

  destroyed() {
    clearTimeout(this._refreshSuggestionsTimer);

    if (this._completionDisposable) {
      this._completionDisposable.dispose();
      this._completionDisposable = null;
    }
  },
};

export default LqlEditorWrapper;
