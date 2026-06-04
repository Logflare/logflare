import * as monaco from "monaco-editor/esm/vs/editor/edcore.main.js";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js";
import {
  registerLqlLanguage,
  registerLqlCompletionProvider,
} from "./lql_language";
import { theme } from "./monaco_editor_theme";

let activeHookCount = 0;
let previousMonacoEnvironment;
let hadPreviousMonacoEnvironment = false;

export function installMonacoEnvironment() {
  if (activeHookCount === 0) {
    hadPreviousMonacoEnvironment = Object.prototype.hasOwnProperty.call(
      window,
      "MonacoEnvironment",
    );
    previousMonacoEnvironment = window.MonacoEnvironment;

    window.MonacoEnvironment = {
      getWorkerUrl() {
        return "/js/monaco_editor_worker.js";
      },
    };
  }

  activeHookCount += 1;
}

export function restoreMonacoEnvironment() {
  if (activeHookCount === 0) return;

  activeHookCount -= 1;

  if (activeHookCount > 0) return;

  if (hadPreviousMonacoEnvironment) {
    window.MonacoEnvironment = previousMonacoEnvironment;
  } else {
    delete window.MonacoEnvironment;
  }

  previousMonacoEnvironment = undefined;
  hadPreviousMonacoEnvironment = false;
}

export function resetMonacoEnvironmentForTests() {
  activeHookCount = 0;
  previousMonacoEnvironment = undefined;
  hadPreviousMonacoEnvironment = false;
  delete window.MonacoEnvironment;
}

export function dispatchInput(formField) {
  formField.dispatchEvent(new Event("input", { bubbles: true }));
}

export function syncFormField(formField, value) {
  formField.value = value;
  dispatchInput(formField);
}

export function submitClosestForm(element) {
  const form = element.closest("form");

  if (!form) return;

  form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
}

export function parseJson(value) {
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

export function loadJsonDataset(context, key, fallback) {
  context.datasetCache = context.datasetCache || {};
  context.datasetValues = context.datasetValues || {};

  const value = context.el.dataset[key];
  const isCached = Object.prototype.hasOwnProperty.call(
    context.datasetCache,
    key,
  );

  if (!isCached || context.datasetCache[key] !== value) {
    context.datasetCache[key] = value;
    context.datasetValues[key] = parseJson(value) || fallback;
  }

  return context.datasetValues[key];
}

export function editorContentHeight(editor, minHeight) {
  return Math.max(editor.getContentHeight(), minHeight);
}

const minEditorHeight = 100;

const MonacoHook = {
  mounted() {
    installMonacoEnvironment();

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
    this.registerLanguage();
    this.registerCompletions();
    this.registerSubmitCommand();
    this.registerFocusEvents();

    this.disposables.push(
      this.editor.onDidChangeModelContent(() => {
        if (this.ignoreEditorChange) return;

        syncFormField(this.formField, this.editor.getValue());
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

    restoreMonacoEnvironment();
  },

  loadDataset() {
    this.language = this.el.dataset.language || "sql";
    this.options = loadJsonDataset(this, "options", {});
    this.completions = loadJsonDataset(this, "completions", []);
    this.schemaFields = loadJsonDataset(this, "schemaFieldsJson", {});
    this.suggestedSearches = loadJsonDataset(
      this,
      "suggestedSearchesJson",
      [],
    );
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
    if (!this.editor || !this.editorContainer) return;

    const height = editorContentHeight(this.editor, this.minEditorHeight);
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
        syncFormField(this.formField, this.editor.getValue());
        submitClosestForm(this.el);
      },
      "!suggestWidgetVisible",
    );
  },
};

export default MonacoHook;
