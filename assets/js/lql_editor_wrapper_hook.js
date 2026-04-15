import {
  registerLqlLanguage,
  registerLqlCompletionProvider,
} from "./lql_language";

const parseSchemaFields = (schemaFieldsJson) => {
  if (!schemaFieldsJson) return {};

  return JSON.parse(schemaFieldsJson);
};

const parseSuggestedSearches = (suggestedSearchesJson) => {
  if (!suggestedSearchesJson) return [];

  return JSON.parse(suggestedSearchesJson);
};

const LqlEditorWrapper = {
  mounted() {
    this._schemaFields = parseSchemaFields(this.el.dataset.schemaFieldsJson);
    this._suggestedSearches = parseSuggestedSearches(
      this.el.dataset.suggestedSearchesJson
    );
    this._completionDisposable = null;
    this._editor = null;
    this._editorDisposables = [];
    this._pendingServerValue = null;
    this._lastServerSetValue = null;
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
      this.applyPendingEditorValue();

      const monaco = window.monaco;

      try {
        registerLqlLanguage(monaco);
      } catch (_) {
        console.log("Failed to register LQL language", _);
      }

      const model = standaloneEditor.getModel();
      monaco.editor.setModelLanguage(model, "lql");

      this._completionDisposable = registerLqlCompletionProvider(
        monaco,
        () => this._schemaFields,
        () => this._suggestedSearches
      );

      standaloneEditor.addCommand(
        monaco.KeyCode.Enter,
        () => {
          this.submitSearch();
        },
        "!suggestWidgetVisible"
      );

      this._editorDisposables = [
        standaloneEditor.onDidChangeModelContent(() => {
          const value = standaloneEditor.getValue();
          if (this._lastServerSetValue !== null && value === this._lastServerSetValue) {
            this._lastServerSetValue = null;
            return;
          }
          this._lastServerSetValue = null;
          this.pushEvent("querystring_changed", { querystring: value });
        }),
        standaloneEditor.onDidFocusEditorText(() => {
          this.pushEvent("form_focus", { value: standaloneEditor.getValue() });
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
    this._suggestedSearches = parseSuggestedSearches(
      this.el.dataset.suggestedSearchesJson
    );
    const value = this.el.dataset.querystring ?? "";

    if (!this._editor) {
      this._pendingServerValue = value;
      return;
    }

    this.setEditorValue(value);
  },

  applyPendingEditorValue() {
    if (!this._editor || this._pendingServerValue === null) {
      return;
    }

    const value = this._pendingServerValue;
    this._pendingServerValue = null;
    this.setEditorValue(value);
  },

  setEditorValue(value) {
    const currentValue = this._editor?.getValue?.();

    if (!this._editor || currentValue === value) {
      return;
    }

    const hadTextFocus = this._editor.hasTextFocus();
    const model = this._editor?.getModel?.();
    const suggestController = this._editor?.getContribution?.(
      "editor.contrib.suggestController"
    );

    this._lastServerSetValue = value;
    this._editor.setValue(value);

    if (hadTextFocus && model) {
      const endPosition = {
        lineNumber: model.getLineCount(),
        column: model.getLineMaxColumn(model.getLineCount()),
      };

      this._editor.setPosition(endPosition);
      suggestController?.cancelSuggestWidget();
    }
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

  disposeEditorBindings() {
    this._editorDisposables.forEach((disposable) => disposable?.dispose?.());
    this._editorDisposables = [];

    if (this._completionDisposable) {
      this._completionDisposable.dispose();
      this._completionDisposable = null;
    }
  },

  destroyed() {
    this.disposeEditorBindings();
  },
};

export default LqlEditorWrapper;
