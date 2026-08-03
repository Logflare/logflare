import { describe, expect, it, vi } from "vitest";

import { getLqlCompletionItems, registerLqlLanguage } from "../lql_language.js";

const COMPLETION_KINDS = {
  Field: "field",
  Keyword: "keyword",
  Module: "module",
  Operator: "operator",
  Snippet: "snippet",
};

const SNIPPET_INSERT_TEXT_RULE = "insert-as-snippet";

function getWord(line, column) {
  let start = column - 1;
  let end = column - 1;

  while (start > 0 && /\w/.test(line[start - 1])) start -= 1;
  while (end < line.length && /\w/.test(line[end])) end += 1;

  return {
    startColumn: start + 1,
    endColumn: end + 1,
  };
}

function getSuggestions(
  line,
  { fields = {}, savedSearches = [] } = {},
  column = line.length + 1,
) {
  return getLqlCompletionItems({
    fields,
    fullLine: line,
    kinds: COMPLETION_KINDS,
    lineMaxColumn: line.length + 1,
    position: {
      column,
      lineNumber: 1,
    },
    savedSearches,
    snippetInsertTextRule: SNIPPET_INSERT_TEXT_RULE,
    textUntilPosition: line.slice(0, column - 1),
    word: getWord(line, column),
  });
}

function buildMonacoLanguageApi(registeredLanguages = []) {
  const register = vi.fn();
  const setMonarchTokensProvider = vi.fn();
  const setLanguageConfiguration = vi.fn();

  return {
    monaco: {
      languages: {
        getLanguages: () => registeredLanguages,
        register,
        setMonarchTokensProvider,
        setLanguageConfiguration,
      },
    },
    register,
    setMonarchTokensProvider,
    setLanguageConfiguration,
  };
}

describe("getLqlCompletionItems", () => {
  it("suggests only matching saved searches for the first token", () => {
    const suggestions = getSuggestions(
      "c",
      {
        savedSearches: [
          "c:count(*) c:group_by(t::hour)",
          "c:count(*) c:group_by(t::day)",
        ],
      },
      2,
    );

    expect(suggestions).toHaveLength(2);
    expect(suggestions.map((suggestion) => suggestion.detail)).toEqual([
      "saved search",
      "saved search",
    ]);
  });

  it("suggests LQL keywords after whitespace", () => {
    const suggestions = getSuggestions("foo ");
    const labels = suggestions.map((suggestion) => suggestion.label);

    expect(labels).toContain("t:");
    expect(labels).toContain("m.");
    expect(labels).toContain("s:");
    expect(labels).toContain("c:group_by");
  });

  it("suggests timestamp values after the shorthand prefix", () => {
    const suggestions = getSuggestions("t:");

    expect(suggestions.map((suggestion) => suggestion.label)).toEqual([
      "today",
      "yesterday",
      "now",
      "last@",
      "this@",
    ]);
  });

  it("suggests group_by values inside c:group_by(", () => {
    const suggestions = getSuggestions("c:group_by(");

    expect(suggestions.map((suggestion) => suggestion.label)).toEqual([
      "t::second",
      "t::minute",
      "t::hour",
      "t::day",
    ]);
  });

  it("suggests filter operators after a field name and colon", () => {
    const suggestions = getSuggestions("m.status:");

    const labels = suggestions.map((suggestion) => suggestion.label);
    expect(labels).toContain(":");
    expect(labels).toContain(":>");
    expect(labels).toContain(":>=");
    expect(labels).toContain(":<");
    expect(labels).toContain(":<=");
    expect(labels).toContain(":~");
    expect(labels).toContain(":@>");
    expect(labels).toContain(":@>~");
    expect(labels).toContain(":..");
    expect(suggestions.map((suggestion) => suggestion.kind)).toEqual(
      suggestions.map(() => COMPLETION_KINDS.Operator),
    );
  });

  it("narrows operator suggestions by typed prefix", () => {
    const suggestions = getSuggestions("m.status:>");

    expect(suggestions.map((suggestion) => suggestion.label)).toEqual([
      ":>",
      ":>=",
    ]);
  });

  it("suggests operators for negated fields", () => {
    const suggestions = getSuggestions("-m.status:");

    expect(suggestions.map((suggestion) => suggestion.label)).toContain(":");
    expect(suggestions.map((suggestion) => suggestion.label)).toContain(":~");
  });

  it("filters keywords by prefix when typing mid-token", () => {
    const suggestions = getSuggestions("foo c:c");

    const labels = suggestions.map((suggestion) => suggestion.label);
    expect(labels).toContain("c:count()");
    expect(labels).toContain("c:countd()");
    expect(labels).not.toContain("c:group_by");
  });

  it("excludes saved search when it is the only match and exactly equals input", () => {
    const suggestions = getSuggestions("m.status:500", {
      savedSearches: ["m.status:500", "m.status:404"],
    });

    expect(suggestions).toHaveLength(0);
  });

  it("keeps saved searches when multiple match even if one is exact", () => {
    const suggestions = getSuggestions("m.status:500", {
      savedSearches: ["m.status:500", "m.status:500 c:count()"],
    });

    expect(suggestions.map((suggestion) => suggestion.label)).toEqual([
      "m.status:500",
      "m.status:500 c:count()",
    ]);
  });

  it("returns no suggestions for unrecognized tokens", () => {
    const suggestions = getSuggestions("foo xyzzy");

    expect(suggestions).toHaveLength(0);
  });

  it("marks snippet keywords with InsertAsSnippet rule", () => {
    const suggestions = getSuggestions("foo ");
    const countSuggestion = suggestions.find(
      (suggestion) => suggestion.label === "c:count()",
    );

    expect(countSuggestion.insertTextRules).toBe(SNIPPET_INSERT_TEXT_RULE);
    expect(countSuggestion.insertText).toBe("c:count($0)");
  });

  it("sets re-suggest command on m. and t: keywords", () => {
    const suggestions = getSuggestions("foo ");
    const mDot = suggestions.find((suggestion) => suggestion.label === "m.");
    const tColon = suggestions.find((suggestion) => suggestion.label === "t:");
    const selectColon = suggestions.find(
      (suggestion) => suggestion.label === "s:",
    );

    expect(mDot.command).toEqual({
      id: "editor.action.triggerSuggest",
      title: "Trigger suggest",
    });
    expect(tColon.command).toEqual({
      id: "editor.action.triggerSuggest",
      title: "Trigger suggest",
    });
    expect(selectColon.command).toBeUndefined();
  });

  it("sets re-suggest command on c:group_by", () => {
    const suggestions = getSuggestions("foo c:g");
    const groupBySuggestion = suggestions.find(
      (suggestion) => suggestion.label === "c:group_by",
    );

    expect(groupBySuggestion.command).toEqual({
      id: "editor.action.triggerSuggest",
      title: "Trigger suggest",
    });
  });

  it("suggests metadata segments and field types from the schema map", () => {
    const fields = {
      "metadata.request.id": "string",
      "metadata.request.method": "string",
      "metadata.status": "integer",
    };

    const rootSuggestions = getSuggestions("m.", { fields });
    const nestedSuggestions = getSuggestions("m.request.", { fields });

    expect(rootSuggestions).toEqual([
      expect.objectContaining({
        detail: "namespace",
        kind: "module",
        label: "request",
      }),
      expect.objectContaining({
        detail: "integer",
        kind: "field",
        label: "status",
      }),
    ]);
    expect(nestedSuggestions).toEqual([
      expect.objectContaining({
        detail: "string",
        kind: "field",
        label: "id",
      }),
      expect.objectContaining({
        detail: "string",
        kind: "field",
        label: "method",
      }),
    ]);
  });

  it("returns no suggestions for an exact single keyword match", () => {
    const suggestions = getSuggestions("foo c:group_by");

    expect(suggestions).toEqual([]);
  });

  it("returns no suggestions for unknown metadata paths", () => {
    const fields = {
      "metadata.request.id": "string",
      "metadata.status": "integer",
    };

    expect(getSuggestions("m.request.zzz.", { fields })).toEqual([]);
  });

  it("returns deduplicated saved searches for blank input using the full line range", () => {
    const suggestions = getSuggestions("", {
      savedSearches: [
        "m.status:500 c:count()",
        "m.status:500 c:count()",
        "t:last@1h m.level:error",
      ],
    });

    expect(suggestions).toEqual([
      expect.objectContaining({
        label: "m.status:500 c:count()",
        detail: "saved search",
        range: {
          startLineNumber: 1,
          endLineNumber: 1,
          startColumn: 1,
          endColumn: 1,
        },
      }),
      expect.objectContaining({
        label: "t:last@1h m.level:error",
        detail: "saved search",
        range: {
          startLineNumber: 1,
          endLineNumber: 1,
          startColumn: 1,
          endColumn: 1,
        },
      }),
    ]);
  });
});

describe("registerLqlLanguage", () => {
  it("registers the language configuration", () => {
    const {
      monaco,
      register,
      setMonarchTokensProvider,
      setLanguageConfiguration,
    } = buildMonacoLanguageApi();

    registerLqlLanguage(monaco);

    expect(register).toHaveBeenCalledWith({ id: "lql" });
    expect(setMonarchTokensProvider).toHaveBeenCalledWith(
      "lql",
      expect.objectContaining({
        ignoreCase: true,
        tokenizer: expect.any(Object),
      }),
    );
    expect(setLanguageConfiguration).toHaveBeenCalledWith("lql", {
      wordPattern: /[a-zA-Z_]\w*/,
    });
  });

  it("returns early when lql is already registered", () => {
    const {
      monaco,
      register,
      setMonarchTokensProvider,
      setLanguageConfiguration,
    } = buildMonacoLanguageApi([{ id: "lql" }]);

    registerLqlLanguage(monaco);

    expect(register).not.toHaveBeenCalled();
    expect(setMonarchTokensProvider).not.toHaveBeenCalled();
    expect(setLanguageConfiguration).not.toHaveBeenCalled();
  });
});
