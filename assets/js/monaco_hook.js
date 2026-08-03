import * as monaco from "monaco-editor/esm/vs/editor/edcore.main.js";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js";
import {
  registerLqlLanguage,
  registerLqlCompletionProvider,
} from "./lql_language";

import { theme } from "./monaco_editor_theme";

const minEditorHeight = 100;

const MonacoHook = {
  mounted() {
    window.MonacoEnvironment = {
      getWorkerUrl() {
        return "/js/monaco_editor_worker.js";
      },
    };
    window.monaco = monaco;

    this.editorContainer = this.el.querySelector("[data-editor-container]");
    this.formField = this.el.querySelector("[data-editor-input]");
    this.loadDataset();
    this.ignoreEditorChange = false;
    this.disposables = [];

    monaco.editor.defineTheme("default", theme);

    const { minHeight: _minHeight, ...configuredOptions } = this.options;

    this.editor = monaco.editor.create(this.editorContainer, {
      ...configuredOptions,
      language: this.language,
      value: this.formField.value,
    });

    this.editorContainer.style.minHeight = `${this.minEditorHeight}px`;
    this.resizeToContent();

    if (this.language === "lql") {
      this.registerLqlLanguage();
      this.registerLqlCompletions();
      this.registerLqlSubmitCommand();
    } else {
      this.registerConfiguredCompletions();
    }

    this.registerFocusEvents();

    this.disposables.push(
      this.editor.onDidChangeModelContent(() => {
        if (this.ignoreEditorChange) return;

        this.syncFormField(this.editor.getValue());
      }),
    );

    this.disposables.push(
      this.editor.onDidContentSizeChange(() => {
        this.resizeToContent();
      }),
    );
  },

  updated() {
    if (!this.editor || !this.formField) return;

    const serverValue = this.formField.value;
    const editorValue = this.editor.getValue();

    if (serverValue === editorValue) {
      return;
    }

    if (this.editor.hasTextFocus()) {
      this.formField.value = editorValue;
      return;
    }

    this.setValue(serverValue);
  },

  destroyed() {
    for (const disposable of this.disposables) {
      disposable.dispose();
    }

    if (this.editor) {
      this.editor.dispose();
    }
  },

  loadDataset() {
    this.language = this.el.dataset.language || "sql";
    this.options = this.loadJsonDataset("options", {});
    this.completions = this.loadJsonDataset("completions", []);
    this.schemaFields = this.loadJsonDataset("schemaFieldsJson", {});
    this.suggestedSearches = this.loadJsonDataset("suggestedSearchesJson", []);
    this.minEditorHeight = this.options.minHeight || minEditorHeight;
    this.emitFocusEvents =
      this.el.dataset.emitFocusEvents !== undefined &&
      this.el.dataset.emitFocusEvents !== "false";
  },

  registerFocusEvents() {
    if (!this.emitFocusEvents) return;

    this.disposables.push(
      this.editor.onDidFocusEditorText(() => {
        this.pushEvent("form_focus", { value: this.editor.getValue() });
      }),
    );

    this.disposables.push(
      this.editor.onDidBlurEditorText(() => {
        this.pushEvent("form_blur", { value: this.editor.getValue() });
      }),
    );
  },

  setValue(value) {
    const model = this.editor.getModel();

    if (!model || model.getValue() === value) return;

    this.ignoreEditorChange = true;
    this.formField.value = value;
    this.editor.executeEdits("server_update", [
      {
        range: model.getFullModelRange(),
        text: value,
      },
    ]);
    this.ignoreEditorChange = false;
  },

  resizeToContent() {
    const height = Math.max(
      this.editor.getContentHeight(),
      this.minEditorHeight,
    );
    this.editorContainer.style.height = `${height}px`;
    this.editor.layout();
  },

  registerLqlLanguage() {
    registerLqlLanguage(monaco);
    monaco.editor.setModelLanguage(this.editor.getModel(), "lql");
  },

  registerLqlCompletions() {
    this.disposables.push(
      registerLqlCompletionProvider(
        monaco,
        () => this.schemaFields,
        () => this.suggestedSearches,
      ),
    );
  },

  registerConfiguredCompletions() {
    const editor = this.editor;

    if (this.completions.length === 0) return;

    const suggestions = this.completions;

    this.disposables.push(
      monaco.languages.registerCompletionItemProvider(this.language, {
        provideCompletionItems(model, position) {
          if (model !== editor.getModel()) {
            return { suggestions: [] };
          }

          const word = model.getWordUntilPosition(position);
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          };

          return {
            suggestions: suggestions.map((name) => ({
              label: name,
              kind: monaco.languages.CompletionItemKind.Module,
              insertText: name,
              range,
            })),
          };
        },
      }),
    );
  },

  registerLqlSubmitCommand() {
    this.editor.addCommand(
      monaco.KeyCode.Enter,
      () => {
        this.syncFormField(this.editor.getValue());
        this.submitClosestForm();
      },
      "!suggestWidgetVisible",
    );
  },

  syncFormField(value) {
    this.formField.value = value;
    this.formField.dispatchEvent(new Event("input", { bubbles: true }));
  },

  submitClosestForm() {
    const form = this.el.closest("form");

    if (form) {
      form.dispatchEvent(
        new Event("submit", { bubbles: true, cancelable: true }),
      );
    }
  },

  loadJsonDataset(key, fallback) {
    const value = this.el.dataset[key];

    try {
      return JSON.parse(value) || fallback;
    } catch (_error) {
      return fallback;
    }
  },
};

export default MonacoHook;
