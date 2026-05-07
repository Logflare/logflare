import * as monaco from "monaco-editor/esm/vs/editor/edcore.main.js";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js";
import {
  registerLqlLanguage,
  registerLqlCompletionProvider,
} from "./lql_language";
import { theme } from "./monaco_editor_theme";

window.MonacoEnvironment = {
  getWorkerUrl() {
    return "/js/monaco_editor_worker.js";
  },
};

function dispatchInput(formField) {
  formField.dispatchEvent(new Event("input", { bubbles: true }));
}

function submitClosestForm(element) {
  const form = element.closest("form");

  if (!form) return;

  if (typeof form.requestSubmit === "function") {
    form.requestSubmit();
  } else {
    form.dispatchEvent(
      new Event("submit", { bubbles: true, cancelable: true }),
    );
  }
}

function parseJson(value) {
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

const editorOptions = {
  automaticLayout: true,
  contextmenu: false,
  editContext: false,
  folding: false,
  fontFamily: "JetBrains Mono, monospace",
  fontSize: 12,
  formatOnPaste: true,
  formatOnType: true,
  glyphMargin: false,
  guides: {
    indentation: false,
  },
  hideCursorInOverviewRuler: true,
  language: "sql",
  lineNumbers: "off",
  lineNumbersMinChars: 0,
  minimap: {
    enabled: false,
  },
  occurrencesHighlight: false,
  padding: {
    top: 14,
    bottom: 14,
  },
  parameterHints: true,
  renderLineHighlight: "none",
  roundedSelection: true,
  scrollbar: {
    vertical: "auto",
    horizontal: "hidden",
    verticalScrollbarSize: 6,
    alwaysConsumeMouseWheel: false,
  },
  scrollBeyondLastLine: false,
  smoothScrolling: true,
  stickyScroll: {
    enabled: false,
  },
  suggestSelection: "first",
  tabCompletion: "on",
  tabIndex: -1,
  tabSize: 2,
  theme: "default",
  wordWrap: "on",
};

const minEditorHeight = 100;

const MonacoHook = {
  mounted() {
    this.editorContainer = this.el.querySelector("[data-editor-container]");
    this.formField = this.el.querySelector("[data-editor-input]");
    this.loadDataset();
    this.ignoreEditorChange = false;
    this.localValue = this.formField.value;
    this.disposables = [];

    if (!this.el.isConnected) return;

    monaco.editor.defineTheme("default", theme);

    const { minHeight: _minHeight, ...configuredOptions } = this.options;

    this.editor = monaco.editor.create(this.editorContainer, {
      ...editorOptions,
      ...configuredOptions,
      language: this.language,
      value: this.formField.value,
    });

    this.editorContainer.style.minHeight = `${this.minEditorHeight}px`;
    this.resizeToContent();
    this.registerLanguage();
    this.registerCompletions();
    this.registerSubmitCommand();

    this.disposables.push(
      this.editor.onDidChangeModelContent(() => {
        if (this.ignoreEditorChange) return;

        this.localValue = this.editor.getValue();
        this.formField.value = this.localValue;
        dispatchInput(this.formField);
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

    this.loadDataset();

    const serverValue = this.formField.value;
    const editorValue = this.editor.getValue();

    if (serverValue === editorValue) {
      this.localValue = serverValue;
      return;
    }

    if (this.editor.hasTextFocus() && this.localValue === editorValue) {
      this.formField.value = editorValue;
      return;
    }

    if (serverValue !== editorValue) {
      this.setValue(serverValue);
    }
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
    this.options = parseJson(this.el.dataset.options) || {};
    this.completions = parseJson(this.el.dataset.completions) || [];
    this.schemaFields = parseJson(this.el.dataset.schemaFieldsJson) || {};
    this.suggestedSearches =
      parseJson(this.el.dataset.suggestedSearchesJson) || [];
    this.minEditorHeight = this.options.minHeight || minEditorHeight;
  },

  setValue(value) {
    const model = this.editor.getModel();

    if (!model || model.getValue() === value) return;

    this.ignoreEditorChange = true;
    this.formField.value = value;
    this.localValue = value;
    this.editor.executeEdits("server_update", [
      {
        range: model.getFullModelRange(),
        text: value,
      },
    ]);
    this.ignoreEditorChange = false;
  },

  resizeToContent() {
    if (!this.editor || !this.editorContainer) return;

    const height = Math.max(
      this.editor.getContentHeight(),
      this.minEditorHeight,
    );
    this.editorContainer.style.height = `${height}px`;
    this.editor.layout();
  },

  registerLanguage() {
    if (this.language !== "lql") return;

    registerLqlLanguage(monaco);
    monaco.editor.setModelLanguage(this.editor.getModel(), "lql");
  },

  registerCompletions() {
    const editor = this.editor;

    if (this.language === "lql") {
      this.disposables.push(
        registerLqlCompletionProvider(
          monaco,
          () => this.schemaFields,
          () => this.suggestedSearches,
        ),
      );

      return;
    }

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

  registerSubmitCommand() {
    if (this.language !== "lql") return;

    this.editor.addCommand(
      monaco.KeyCode.Enter,
      () => {
        this.localValue = this.editor.getValue();
        this.formField.value = this.localValue;
        dispatchInput(this.formField);
        submitClosestForm(this.el);
      },
      "!suggestWidgetVisible",
    );
  },
};

export default MonacoHook;
