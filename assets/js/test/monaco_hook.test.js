import { beforeEach, describe, expect, it, vi } from "vitest";

const defineTheme = vi.fn();
const create = vi.fn();
const setModelLanguage = vi.fn();
const registerCompletionItemProvider = vi.fn();
const register = vi.fn();
const setMonarchTokensProvider = vi.fn();
const setLanguageConfiguration = vi.fn();
let editor;
let model;

vi.stubGlobal("window", {});

vi.mock("monaco-editor/esm/vs/editor/edcore.main.js", () => ({
  editor: {
    create,
    defineTheme,
    setModelLanguage,
  },
  languages: {
    CompletionItemKind: {
      Field: "field",
      Keyword: "keyword",
      Module: "module",
      Operator: "operator",
      Snippet: "snippet",
    },
    CompletionItemInsertTextRule: {
      InsertAsSnippet: "insert-as-snippet",
    },
    getLanguages: () => [],
    register,
    registerCompletionItemProvider,
    setLanguageConfiguration,
    setMonarchTokensProvider,
  },
  KeyCode: {
    Enter: "Enter",
  },
}));

vi.mock(
  "monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js",
  () => ({}),
);
vi.mock("../monaco_editor_theme", () => ({ theme: {} }));

const monacoHookModule = await import("../monaco_hook.js");
const { default: MonacoHook } = monacoHookModule;

function buildHook({ language = "lql", form, dataset = {} } = {}) {
  const editorContainer = { style: {} };
  const input = {
    dispatchEvent: vi.fn(),
    value: "m.level:error",
  };
  const root = {
    dataset: {
      language,
      options: "{}",
      completions: "[]",
      schemaFieldsJson: "{}",
      suggestedSearchesJson: "[]",
      ...dataset,
    },
    isConnected: true,
    closest: vi.fn(() => form),
    querySelector: vi.fn((selector) => {
      if (selector === "[data-editor-container]") return editorContainer;
      if (selector === "[data-editor-input]") return input;
      return null;
    }),
  };
  const hook = { ...MonacoHook, el: root };

  return { editorContainer, hook, input, root };
}

beforeEach(() => {
  vi.clearAllMocks();
  delete window.MonacoEnvironment;

  model = {
    getFullModelRange: vi.fn(() => "full-range"),
    getValue: vi.fn(() => "m.level:error"),
  };

  editor = {
    addCommand: vi.fn(),
    dispose: vi.fn(),
    executeEdits: vi.fn(),
    getContentHeight: vi.fn(() => 32),
    getModel: vi.fn(() => model),
    getValue: vi.fn(() => "m.level:error"),
    hasTextFocus: vi.fn(() => false),
    layout: vi.fn(),
    onDidChangeModelContent: vi.fn(() => ({ dispose: vi.fn() })),
    onDidContentSizeChange: vi.fn(() => ({ dispose: vi.fn() })),
    onDidBlurEditorText: vi.fn(() => ({ dispose: vi.fn() })),
    onDidFocusEditorText: vi.fn(() => ({ dispose: vi.fn() })),
  };

  create.mockReturnValue(editor);
  registerCompletionItemProvider.mockReturnValue({ dispose: vi.fn() });
});

describe("MonacoHook", () => {
  describe("MonacoEnvironment management", () => {
    it("creates MonacoEnvironment during mount", () => {
      expect(window.MonacoEnvironment).toBeUndefined();

      const { hook } = buildHook({ language: "sql" });
      hook.mounted();
      expect(window.MonacoEnvironment.getWorkerUrl()).toBe(
        "/js/monaco_editor_worker.js",
      );
    });
  });

  describe("mounting and basic events", () => {
    it("creates the editor with merged options and keeps minHeight off Monaco options", () => {
      const { editorContainer, hook, input } = buildHook({
        language: "sql",
        dataset: {
          options: JSON.stringify({
            fontSize: 16,
            minHeight: 240,
            scrollbar: { horizontal: "auto" },
          }),
        },
      });

      hook.mounted();

      expect(defineTheme).toHaveBeenCalledWith("default", {});
      expect(create).toHaveBeenCalledWith(
        editorContainer,
        expect.objectContaining({
          fontSize: 16,
          language: "sql",
          scrollbar: { horizontal: "auto" },
          value: input.value,
        }),
      );
      expect(create.mock.calls[0][1]).not.toHaveProperty("minHeight");
      expect(editorContainer.style.minHeight).toBe("240px");
    });

    it("syncs editor changes into the hidden form field", () => {
      const { hook, input } = buildHook({ language: "sql" });

      hook.mounted();
      editor.getValue.mockReturnValue("select * from events");

      const [onChange] = editor.onDidChangeModelContent.mock.calls[0];
      onChange();

      expect(input.value).toBe("select * from events");
      expect(input.dispatchEvent).toHaveBeenCalledWith(
        expect.objectContaining({ bubbles: true, type: "input" }),
      );
    });

    it("pushes focus and blur events with the current editor value", () => {
      const { hook } = buildHook({
        language: "sql",
        dataset: { emitFocusEvents: "" },
      });
      hook.pushEvent = vi.fn();

      hook.mounted();
      editor.getValue.mockReturnValue("select * from events");

      const [onFocus] = editor.onDidFocusEditorText.mock.calls[0];
      const [onBlur] = editor.onDidBlurEditorText.mock.calls[0];
      onFocus();
      onBlur();

      expect(hook.pushEvent).toHaveBeenCalledWith("form_focus", {
        value: "select * from events",
      });
      expect(hook.pushEvent).toHaveBeenCalledWith("form_blur", {
        value: "select * from events",
      });
    });

    it("does not register focus and blur events by default", () => {
      const { hook } = buildHook({ language: "sql" });

      hook.mounted();

      expect(editor.onDidFocusEditorText).not.toHaveBeenCalled();
      expect(editor.onDidBlurEditorText).not.toHaveBeenCalled();
    });

    it("does not register focus and blur events when explicitly disabled", () => {
      const { hook } = buildHook({
        language: "sql",
        dataset: { emitFocusEvents: "false" },
      });

      hook.mounted();

      expect(editor.onDidFocusEditorText).not.toHaveBeenCalled();
      expect(editor.onDidBlurEditorText).not.toHaveBeenCalled();
    });
  });

  describe("hook updates", () => {
    it("ignores update before the editor is mounted", () => {
      const { hook } = buildHook({ language: "sql" });
      hook.updated();
      expect(editor.getValue).not.toHaveBeenCalled();
    });

    it("does nothing when form field and editor values match", () => {
      const { hook, input } = buildHook({ language: "sql" });
      hook.mounted();
      editor.getValue.mockReturnValue(input.value);
      hook.updated();
      expect(editor.executeEdits).not.toHaveBeenCalled();
    });

    it("keeps focused editor content and updates form field when patch has stale content", () => {
      const { hook, input } = buildHook({ language: "sql" });
      hook.mounted();
      editor.hasTextFocus.mockReturnValue(true);
      editor.getValue.mockReturnValue("select fresh");
      input.value = "select stale";

      hook.updated();

      expect(input.value).toBe("select fresh");
      expect(editor.executeEdits).not.toHaveBeenCalled();
    });

    it("applies changed form field content when the editor is not focused", () => {
      const { hook, input } = buildHook({ language: "sql" });
      hook.mounted();
      editor.hasTextFocus.mockReturnValue(false);
      editor.getValue.mockReturnValue("select old");
      model.getValue.mockReturnValue("select old");
      input.value = "select new";

      hook.updated();

      expect(editor.executeEdits).toHaveBeenCalledWith("server_update", [
        { range: "full-range", text: "select new" },
      ]);
    });
  });

  describe("setValue", () => {
    let hook;
    let input;

    beforeEach(() => {
      ({ hook, input } = buildHook({ language: "sql" }));
      hook.mounted();
    });

    it("does not edit when there is no model or the model already has the value", () => {
      editor.getModel.mockReturnValue(null);
      hook.setValue("select new");

      editor.getModel.mockReturnValue(model);
      model.getValue.mockReturnValue("select new");
      hook.setValue("select new");

      expect(editor.executeEdits).not.toHaveBeenCalled();
    });

    it("ignores editor change events while applying server values", () => {
      editor.getValue.mockReturnValue("select from callback");
      model.getValue.mockReturnValue("select old");

      editor.executeEdits.mockImplementation(() => {
        const [onChange] = editor.onDidChangeModelContent.mock.calls[0];
        onChange();
      });

      hook.setValue("select new");

      expect(input.value).toBe("select new");
      expect(input.dispatchEvent).not.toHaveBeenCalledWith(
        expect.objectContaining({ type: "input" }),
      );
    });
  });

  describe("resizeToContent", () => {
    it("resizes to the larger of content height and min height", () => {
      const { editorContainer, hook } = buildHook({
        language: "sql",
        dataset: { options: JSON.stringify({ minHeight: 120 }) },
      });

      editor.getContentHeight.mockReturnValue(240);
      hook.mounted();

      expect(editorContainer.style.height).toBe("240px");
      expect(editor.layout).toHaveBeenCalled();
    });

  });

  describe("disposal and destruction", () => {
    it("disposes registered subscriptions and the editor", () => {
      const disposables = [
        { dispose: vi.fn() },
        { dispose: vi.fn() },
        { dispose: vi.fn() },
      ];
      editor.onDidChangeModelContent.mockReturnValue(disposables[0]);
      editor.onDidContentSizeChange.mockReturnValue(disposables[1]);
      registerCompletionItemProvider.mockReturnValue(disposables[2]);

      const { hook } = buildHook();
      hook.mounted();
      hook.destroyed();

      disposables.forEach((d) => expect(d.dispose).toHaveBeenCalled());
      expect(editor.dispose).toHaveBeenCalled();
    });

    it("does not fail destroying when no editor was created", () => {
      const { hook } = buildHook();
      hook.disposables = [{ dispose: vi.fn() }];

      expect(() => hook.destroyed()).not.toThrow();
      expect(editor.dispose).not.toHaveBeenCalled();
    });
  });

  describe("LQL language features", () => {
    it("registers LQL language support", () => {
      const { hook } = buildHook({ language: "lql" });
      hook.registerLqlLanguage = vi.fn(hook.registerLqlLanguage);

      hook.mounted();

      expect(hook.registerLqlLanguage).toHaveBeenCalled();
      expect(register).toHaveBeenCalledWith({ id: "lql" });
      expect(setMonarchTokensProvider).toHaveBeenCalledWith(
        "lql",
        expect.any(Object),
      );
      expect(setModelLanguage).toHaveBeenCalledWith(model, "lql");
    });

    it("dispatches a submit event when the LQL Enter command runs", () => {
      const form = { dispatchEvent: vi.fn(), requestSubmit: vi.fn() };
      const { hook } = buildHook({ language: "lql", form });

      hook.mounted();

      const [, submit] = editor.addCommand.mock.calls[0];
      submit();

      expect(form.dispatchEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          bubbles: true,
          cancelable: true,
          type: "submit",
        }),
      );
      expect(form.requestSubmit).not.toHaveBeenCalled();
    });

    it("uses mounted LQL schema fields in completion providers", () => {
      const { hook } = buildHook({
        language: "lql",
        dataset: {
          schemaFieldsJson: JSON.stringify({ "metadata.level": "string" }),
        },
      });
      hook.registerLqlCompletions = vi.fn(hook.registerLqlCompletions);

      hook.mounted();

      expect(hook.registerLqlCompletions).toHaveBeenCalled();
      const provider = registerCompletionItemProvider.mock.calls[0][1];

      const suggestions = provider.provideCompletionItems(
        {
          getLineContent: () => "m.",
          getLineMaxColumn: () => 3,
          getValueInRange: () => "m.",
          getWordUntilPosition: () => ({ startColumn: 1, endColumn: 3 }),
        },
        { column: 3, lineNumber: 1 },
      ).suggestions;

      expect(suggestions).toEqual([
        expect.objectContaining({ detail: "string", label: "level" }),
      ]);
    });
  });

  describe("SQL language features", () => {
    it("does not register an Enter submit command for SQL editors", () => {
      const { hook } = buildHook({ language: "sql" });
      hook.registerLqlLanguage = vi.fn(hook.registerLqlLanguage);
      hook.registerLqlCompletions = vi.fn(hook.registerLqlCompletions);
      hook.registerLqlSubmitCommand = vi.fn(hook.registerLqlSubmitCommand);

      hook.mounted();

      expect(hook.registerLqlLanguage).not.toHaveBeenCalled();
      expect(hook.registerLqlCompletions).not.toHaveBeenCalled();
      expect(hook.registerLqlSubmitCommand).not.toHaveBeenCalled();
      expect(editor.addCommand).not.toHaveBeenCalled();
    });

    it("registers SQL completions for the active editor model only", () => {
      const { hook } = buildHook({
        language: "sql",
        dataset: { completions: JSON.stringify(["events", "sources"]) },
      });
      hook.registerConfiguredCompletions = vi.fn(
        hook.registerConfiguredCompletions,
      );

      hook.mounted();

      expect(hook.registerConfiguredCompletions).toHaveBeenCalled();
      const provider = registerCompletionItemProvider.mock.calls[0][1];
      const position = { column: 3, lineNumber: 1 };
      model.getWordUntilPosition = vi.fn(() => ({
        endColumn: 3,
        startColumn: 1,
      }));

      expect(provider.provideCompletionItems({}, position)).toEqual({
        suggestions: [],
      });

      const completions = provider.provideCompletionItems(model, position);
      expect(completions.suggestions).toEqual([
        expect.objectContaining({
          insertText: "events",
          kind: "module",
          label: "events",
          range: {
            endColumn: 3,
            endLineNumber: 1,
            startColumn: 1,
            startLineNumber: 1,
          },
        }),
        expect.objectContaining({ label: "sources" }),
      ]);
    });

    it("does not register SQL completions when none are configured", () => {
      const { hook } = buildHook({ language: "sql" });
      hook.mounted();
      expect(registerCompletionItemProvider).not.toHaveBeenCalled();
    });
  });
});

describe("MonacoHook helpers", () => {
  describe("loadJsonDataset", () => {
    it("parses dataset JSON", () => {
      const hook = {
        ...MonacoHook,
        el: { dataset: { options: JSON.stringify({ minHeight: 120 }) } },
      };

      expect(hook.loadJsonDataset("options", {})).toEqual({ minHeight: 120 });
    });

    it("falls back when dataset JSON is missing or invalid", () => {
      const hook = { ...MonacoHook, el: { dataset: { options: "{" } } };
      const fallback = { minHeight: 100 };

      expect(hook.loadJsonDataset("options", fallback)).toBe(fallback);

      hook.el.dataset = {};
      expect(hook.loadJsonDataset("missing", [])).toEqual([]);
    });
  });

  it("syncs a form field value and dispatches input", () => {
    const hook = {
      ...MonacoHook,
      formField: { dispatchEvent: vi.fn(), value: "" },
    };

    hook.syncFormField("select 1");

    expect(hook.formField.value).toBe("select 1");
    expect(hook.formField.dispatchEvent).toHaveBeenCalledWith(
      expect.objectContaining({ bubbles: true, type: "input" }),
    );
  });

  describe("submitClosestForm", () => {
    it("submits the closest form if one exists, or does nothing if not found", () => {
      const form = { dispatchEvent: vi.fn() };
      const hook = { ...MonacoHook, el: { closest: vi.fn(() => form) } };

      hook.submitClosestForm();
      expect(hook.el.closest).toHaveBeenCalledWith("form");
      expect(form.dispatchEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          bubbles: true,
          cancelable: true,
          type: "submit",
        }),
      );

      hook.el.closest.mockReturnValue(null);
      expect(() => hook.submitClosestForm()).not.toThrow();
    });
  });
});
