import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js";
import { theme } from "./monaco_editor_theme";

window.MonacoEnvironment = {
  getWorkerUrl() {
    return "/js/monaco_editor_worker.js";
  },
};

function dispatchInput(textarea) {
  textarea.dispatchEvent(new Event("input", { bubbles: true }));
}

function parseCompletions(value) {
  if (!value) return [];

  try {
    return JSON.parse(value);
  } catch (_error) {
    return [];
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
    this.textarea = this.el.querySelector("textarea");
    this.completions = parseCompletions(this.el.dataset.completions);
    this.ignoreEditorChange = false;
    this.disposables = [];

    if (!this.el.isConnected) return;

    monaco.editor.defineTheme("default", theme);

    this.editor = monaco.editor.create(this.editorContainer, {
      ...editorOptions,
      value: this.textarea.value,
    });

    this.editorContainer.style.minHeight = `${minEditorHeight}px`;
    this.resizeToContent();
    this.registerCompletions();

    this.disposables.push(
      this.editor.onDidChangeModelContent(() => {
        if (this.ignoreEditorChange) return;

        this.textarea.value = this.editor.getValue();
        dispatchInput(this.textarea);
      }),
    );

    this.disposables.push(
      this.editor.onDidContentSizeChange(() => {
        this.resizeToContent();
      }),
    );
  },

  updated() {
    if (!this.editor || !this.textarea) return;

    const serverValue = this.textarea.value;
    const editorValue = this.editor.getValue();

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

  setValue(value) {
    const model = this.editor.getModel();

    if (!model || model.getValue() === value) return;

    this.ignoreEditorChange = true;
    this.textarea.value = value;
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

    const height = Math.max(this.editor.getContentHeight(), minEditorHeight);
    this.editorContainer.style.height = `${height}px`;
    this.editor.layout();
  },

  registerCompletions() {
    if (this.completions.length === 0) return;

    const editor = this.editor;
    const suggestions = this.completions;

    this.disposables.push(
      monaco.languages.registerCompletionItemProvider("sql", {
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
};

export default MonacoHook;
