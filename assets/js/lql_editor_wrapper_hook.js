import {
  registerLqlLanguage,
  registerLqlCompletionProvider,
} from "./lql_language";

const parseSchemaFields = (schemaFieldsJson) => {
  if (!schemaFieldsJson) return [];

  return JSON.parse(schemaFieldsJson);
};

const LqlEditorWrapper = {
  mounted() {
    this._schemaFields = parseSchemaFields(this.el.dataset.schemaFieldsJson);
    this._suggestedSearches = [];
    this._completionDisposable = null;
    this._editor = null;
    this._editorDisposables = [];
    this._handleSubmitRequest = () => {
      this.submitSearch();
    };
    this._handleEditorMounted = (event) => {
      const { editor } = event.detail;
      const standaloneEditor = editor.standalone_code_editor;

      if (this._editor === standaloneEditor && this._completionDisposable) {
        return;
      }

      this.disposeEditorBindings();
      this._editor = standaloneEditor;

      const monaco = window.monaco;

      registerLqlLanguage(monaco);
      const model = standaloneEditor.getModel();
      monaco.editor.setModelLanguage(model, "lql");

      this._completionDisposable = registerLqlCompletionProvider(
        monaco,
        () => this._schemaFields,
        () => this._suggestedSearches,
      );

      standaloneEditor.addCommand(
        monaco.KeyCode.Enter,
        () => {
          this.submitSearch();
        },
        "!suggestWidgetVisible",
      );

      this._editorDisposables = [
        standaloneEditor.onDidChangeModelContent(() => {
          const value = standaloneEditor.getValue();
          this.pushEvent("querystring_changed", { querystring: value });
        }),
        standaloneEditor.onDidFocusEditorText(() => {
          this.pushEvent(
            "form_focus",
            { value: standaloneEditor.getValue() },
            ({ suggestions = [] }) => {
              this._suggestedSearches = Array.isArray(suggestions)
                ? suggestions
                : [];

              const editorValue = standaloneEditor.getValue() ?? "";

              if (
                this._suggestedSearches.length > 0 ||
                editorValue.trim().length === 0
              ) {
                this.refreshSuggestions();
              }
            },
          );
        }),
        standaloneEditor.onDidBlurEditorText(() => {
          this.pushEvent("form_blur", { value: standaloneEditor.getValue() });
        }),
      ];
    };

    this.el.addEventListener("lql:submit", this._handleSubmitRequest);
    this.el.addEventListener("lme:editor_mounted", this._handleEditorMounted);
  },

  updated() {
    this._schemaFields = parseSchemaFields(this.el.dataset.schemaFieldsJson);
    this.restoreCursorToEndIfNeeded();
  },

  restoreCursorToEndIfNeeded(attempt = 0) {
    window.requestAnimationFrame(() => {
      const serverQuerystring = this.el.dataset.querystring ?? "";
      const editorValue = this._editor?.getValue?.();
      const model = this._editor?.getModel?.();
      const position = this._editor?.getPosition?.();
      const suggestController = this._editor?.getContribution?.(
        "editor.contrib.suggestController",
      );

      if (!this._editor || !model || !position) {
        return;
      }

      if (serverQuerystring !== editorValue) {
        if (attempt < 6) {
          window.setTimeout(() => {
            this.restoreCursorToEndIfNeeded(attempt + 1);
          }, 50);
        }

        return;
      }

      if (editorValue.length === 0) {
        return;
      }

      const endPosition = {
        lineNumber: model.getLineCount(),
        column: model.getLineMaxColumn(model.getLineCount()),
      };
      const hasTextFocus = this._editor.hasTextFocus();

      if (hasTextFocus && position.lineNumber === 1 && position.column === 1) {
        this._editor.setPosition(endPosition);
        suggestController?.cancelSuggestWidget?.();
        return;
      }

      if (!hasTextFocus && attempt < 6) {
        window.setTimeout(() => {
          this.restoreCursorToEndIfNeeded(attempt + 1);
        }, 50);
      }
    });
  },

  collectRecommendedFields() {
    const searchControl =
      this.el.closest("#source-logs-search-control") || this.el.parentElement;
    const fields = {};

    searchControl
      ?.querySelectorAll('#recommended_fields input[name^="fields["]')
      .forEach((input) => {
        const match = input.name.match(/^fields\[(.+)\]$/);

        if (match) {
          fields[match[1]] = input.value;
        }
      });

    return fields;
  },

  submitSearch() {
    const querystring = this._editor?.getValue?.() ?? "";

    this.pushEvent("start_search", {
      querystring,
      fields: this.collectRecommendedFields(),
    });
  },

  refreshSuggestions() {
    const suggestController = this._editor?.getContribution?.(
      "editor.contrib.suggestController",
    );

    suggestController?.cancelSuggestWidget?.();

    if (suggestController?.triggerSuggest) {
      suggestController.triggerSuggest();
      return;
    }

    this._editor?.getAction("editor.action.triggerSuggest")?.run();
  },

  disposeEditorBindings() {
    this._editorDisposables.forEach((disposable) => disposable?.dispose?.());
    this._editorDisposables = [];

    if (this._completionDisposable) {
      this._completionDisposable.dispose();
      this._completionDisposable = null;
    }
  },

  destroyed() {
    this.el.removeEventListener("lql:submit", this._handleSubmitRequest);
    this.el.removeEventListener(
      "lme:editor_mounted",
      this._handleEditorMounted,
    );
    this.disposeEditorBindings();
    this._editor = null;
  },
};

export default LqlEditorWrapper;
