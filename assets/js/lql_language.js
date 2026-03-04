/**
 * LQL syntax definition for Monaco Editor.
 */

export function registerLqlLanguage(monaco) {
  if (monaco.languages.getLanguages().some((lang) => lang.id === "lql")) {
    return;
  }

  monaco.languages.register({ id: "lql" });

  monaco.languages.setMonarchTokensProvider("lql", {
    ignoreCase: true,

    keywords: ["true", "false", "NULL"],

    logLevels: [
      "emergency",
      "alert",
      "critical",
      "error",
      "warning",
      "notice",
      "info",
      "debug",
    ],

    functions: [
      "count",
      "countd",
      "avg",
      "sum",
      "max",
      "p50",
      "p95",
      "p99",
      "group_by",
    ],

    tokenizer: {
      root: [
        // Chart/select/from prefixes
        [
          /\b((?:timestamp|t):)(:(?:minute|min|m|second|s|hour|h|day|d)\b)/,
          ["keyword.prefix.timestamp", "constant.period"],
        ],
        [/(?:chart|c|select|s|from|f|timestamp|t):/, "keyword.prefix"],

        // Metadata prefix (m. or metadata.)
        [/(?:metadata|m)\./, "keyword.prefix", "@metadataPath"],

        // Operators (order matters: longest first)
        [/:@>~/, "operator"],
        [/:@>/, "operator"],
        [/:>=/, "operator"],
        [/:<=/, "operator"],
        [/:>/, "operator"],
        [/:</, "operator"],
        [/:~/, "operator"],
        [/:=/, "operator"],
        [/:\.\./, "operator"],
        [/\.\./, "operator"],
        [/:/, "operator"],

        // Negation prefix
        [/-(?=\w)/, "operator.negation"],

        // Quoted strings
        [/"/, "string", "@string"],

        // Regex patterns: ~"..." or ~word
        [/~"/, "regexp", "@regexpQuoted"],
        [/~\S+/, "regexp"],

        // Timestamp shorthands: last@5m, this@week, today, yesterday, now
        [/(?:last|this)@\w+/, "number.date"],
        [/\b(?:today|yesterday|now)\b/, "number.date"],

        // ISO-ish dates: 2024-01-15T00:00:00 or 2024-01-15
        [
          /\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?/,
          "number.date",
        ],

        // Functions (followed by open paren)
        [
          /\b([a-zA-Z_]\w*)\s*(?=\()/,
          {
            cases: {
              "@functions": "function",
              "@default": "identifier",
            },
          },
        ],

        // Constants
        [
          /\b\w+\b/,
          {
            cases: {
              "@keywords": "constant",
              "@logLevels": "type.loglevel",
              "@default": "identifier",
            },
          },
        ],

        // Numbers
        [/\d+\.\d+/, "number.float"],
        [/\d+/, "number"],

        // Whitespace
        [/\s+/, "white"],
      ],

      metadataPath: [
        [/[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*/, "variable", "@pop"],
        [/(?:)/, "", "@pop"],
      ],

      string: [
        [/[^"]+/, "string"],
        [/"/, "string", "@pop"],
      ],

      regexpQuoted: [
        [/[^"]+/, "regexp"],
        [/"/, "regexp", "@pop"],
      ],
    },
  });

  monaco.languages.setLanguageConfiguration("lql", {
    wordPattern: /[a-zA-Z_]\w*/,
  });
}

const LQL_KEYWORDS = [
  { label: "t:", detail: "timestamp filter", insertText: "t:" },
  { label: "m.", detail: "metadata field", insertText: "m." },
  { label: "c:count(*)", detail: "chart count", insertText: "c:count(*)" },
  {
    label: "c:group_by(t::minute)",
    detail: "chart group by minute",
    insertText: "c:group_by(t::minute)",
  },
  {
    label: "c:group_by(t::hour)",
    detail: "chart group by hour",
    insertText: "c:group_by(t::hour)",
  },
  { label: "c:avg()", detail: "chart average", insertText: "c:avg($0)" },
  { label: "c:sum()", detail: "chart sum", insertText: "c:sum($0)" },
  { label: "c:max()", detail: "chart max", insertText: "c:max($0)" },
  { label: "c:p50()", detail: "chart p50", insertText: "c:p50($0)" },
  { label: "c:p95()", detail: "chart p95", insertText: "c:p95($0)" },
  { label: "c:p99()", detail: "chart p99", insertText: "c:p99($0)" },
  { label: "s:", detail: "select fields", insertText: "s:" },
  { label: "f:", detail: "from source", insertText: "f:" },
];

/**
 * Registers a CompletionItemProvider for the LQL language.
 * @param {object} monaco - The monaco-editor namespace
 * @param {function} getFields - Returns current schema fields array [{name, type}]
 * @returns {IDisposable} The disposable for the registered provider
 */
export function registerLqlCompletionProvider(monaco, getFields) {
  return monaco.languages.registerCompletionItemProvider("lql", {
    triggerCharacters: ["."],

    provideCompletionItems(model, position) {
      const textUntilPosition = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      });

      const word = model.getWordUntilPosition(position);
      const replaceRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      // Check if we're in a metadata path context: m. or metadata. prefix
      const metaMatch = textUntilPosition.match(
        /(?:^|[\s:])(?:m|metadata)\.([\w.]*?)$/,
      );

      if (metaMatch) {
        const typedPath = metaMatch[1]; // e.g. "request." or "req"
        const fields = getFields();
        const metadataFields = fields.filter((f) =>
          f.name.startsWith("metadata."),
        );

        // Determine what prefix is already typed after m./metadata.
        // e.g. if user typed "m.request.", typedPath = "request."
        // We want to show fields under "metadata.request."
        const pathPrefix = typedPath.includes(".")
          ? typedPath.substring(0, typedPath.lastIndexOf(".") + 1)
          : "";
        const fullPrefix = "metadata." + pathPrefix;

        // Collect unique next segments at this level
        const seen = new Set();
        const suggestions = [];

        for (const field of metadataFields) {
          if (!field.name.startsWith(fullPrefix)) continue;

          const remainder = field.name.slice(fullPrefix.length);
          if (!remainder) continue;

          const dotIdx = remainder.indexOf(".");
          const segment =
            dotIdx >= 0 ? remainder.substring(0, dotIdx) : remainder;
          const isLeaf = dotIdx < 0;

          if (seen.has(segment)) continue;
          seen.add(segment);

          suggestions.push({
            label: segment,
            kind: isLeaf
              ? monaco.languages.CompletionItemKind.Field
              : monaco.languages.CompletionItemKind.Module,
            detail: isLeaf ? field.type : "namespace",
            insertText: segment,
            range: replaceRange,
            sortText: segment.padStart(50),
          });
        }

        return { suggestions };
      }

      // At start of input or after whitespace: suggest LQL keywords
      if (/(?:^|[\s])$/.test(textUntilPosition)) {
        const suggestions = LQL_KEYWORDS.map((kw, i) => ({
          label: kw.label,
          kind: monaco.languages.CompletionItemKind.Keyword,
          detail: kw.detail,
          insertText: kw.insertText,
          insertTextRules: kw.insertText.includes("$0")
            ? monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
            : undefined,
          range: replaceRange,
          sortText: String(i).padStart(3, "0"),
        }));

        return { suggestions };
      }

      return { suggestions: [] };
    },
  });
}
