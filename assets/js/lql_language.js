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
        [/[{}]/, "operator"],
        [/\.\./, "operator"],
        [/:/, "operator"],

        // Negation prefix, only at the start of a filter token
        [
          /(^|\s)(-)(?=(?:(?:m|metadata)\.[a-zA-Z_]\w*|(?:t|timestamp)\b|[a-zA-Z_]\w*))/,
          ["white", "operator.negation"],
        ],

        // Quoted strings
        [/"/, "string", "@string"],

        // Regex patterns: ~"..." or ~word
        [/~"/, "regexp", "@regexpQuoted"],
        [/~\S+/, "regexp"],

        // Timestamp shorthands: last@5m, this@week, today, yesterday, now
        [/(?:last|this)@\w+/, "number.date"],
        [/\b(?:today|yesterday|now)\b/, "number.date"],

        // dates: 2024-01-15T00:00:00 or 2024-01-15
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
  { label: "timestamp:", detail: "timestamp filter", insertText: "timestamp:" },
  { label: "m.", detail: "metadata field", insertText: "m." },
  { label: "metadata.", detail: "metadata field", insertText: "metadata." },
  { label: "s:*", detail: "select all fields", insertText: "s:*" },
  { label: "c:count(*)", detail: "chart count", insertText: "c:count(*)" },
  {
    label: "c:countd()",
    detail: "chart distinct count",
    insertText: "c:countd($0)",
  },
  {
    label: "c:group_by(t::minute)",
    detail: "chart group by minute",
    insertText: "c:group_by(t::minute)",
  },
  {
    label: "c:group_by(t::second)",
    detail: "chart group by second",
    insertText: "c:group_by(t::second)",
  },
  {
    label: "c:group_by(t::hour)",
    detail: "chart group by hour",
    insertText: "c:group_by(t::hour)",
  },
  {
    label: "c:group_by(t::day)",
    detail: "chart group by day",
    insertText: "c:group_by(t::day)",
  },
  { label: "c:avg()", detail: "chart average", insertText: "c:avg($0)" },
  { label: "c:sum()", detail: "chart sum", insertText: "c:sum($0)" },
  { label: "c:max()", detail: "chart max", insertText: "c:max($0)" },
  { label: "c:p50()", detail: "chart p50", insertText: "c:p50($0)" },
  { label: "c:p95()", detail: "chart p95", insertText: "c:p95($0)" },
  { label: "c:p99()", detail: "chart p99", insertText: "c:p99($0)" },
  { label: "s:", detail: "select fields", insertText: "s:" },
  { label: "select:", detail: "select fields", insertText: "select:" },
  { label: "f:", detail: "from source", insertText: "f:" },
  { label: "from:", detail: "from source", insertText: "from:" },
];

const LQL_TIMESTAMP_KEYWORDS = [
  { label: "today", detail: "today", insertText: "today" },
  { label: "yesterday", detail: "yesterday", insertText: "yesterday" },
  { label: "now", detail: "current time", insertText: "now" },
  { label: "last@", detail: "relative past time", insertText: "last@" },
  { label: "this@", detail: "current time period", insertText: "this@" },
];

const LQL_FILTER_OPERATORS = [
  { label: ":", detail: "exact match", insertText: ":" },
  { label: ":..", detail: "range operator", insertText: ":.." },
  { label: ":>", detail: "greater than", insertText: ":>" },
  { label: ":>=", detail: "greater than or equal", insertText: ":>=" },
  { label: ":<", detail: "less than", insertText: ":<" },
  { label: ":<=", detail: "less than or equal", insertText: ":<=" },
  { label: ":~", detail: "regex match", insertText: ":~" },
  { label: ":@>", detail: "array includes value", insertText: ":@>" },
  { label: ":@>~", detail: "array includes regex match", insertText: ":@>~" },
];

function buildKeywordSuggestions(monaco, token, range) {
  return LQL_KEYWORDS.filter(
    (kw) =>
      kw.label.toLocaleLowerCase().startsWith(token.toLocaleLowerCase()) ||
      kw.insertText.toLocaleLowerCase().startsWith(token.toLocaleLowerCase()),
  ).map((kw, i) => ({
    label: kw.label,
    kind: monaco.languages.CompletionItemKind.Keyword,
    detail: kw.detail,
    insertText: kw.insertText,
    insertTextRules: kw.insertText.includes("$0")
      ? monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
      : undefined,
    command:
      kw.insertText === "m." || kw.insertText === "metadata."
        ? {
            id: "editor.action.triggerSuggest",
            title: "Trigger suggest",
          }
        : undefined,
    range,
    sortText: String(i).padStart(3, "0"),
  }));
}

function hasSingleExactKeywordSuggestion(token, suggestions) {
  return (
    suggestions.length === 1 &&
    suggestions[0].label.toLocaleLowerCase() === token.toLocaleLowerCase()
  );
}

function buildOperatorSuggestions(monaco, operatorPrefix, range) {
  return LQL_FILTER_OPERATORS.filter((operator) =>
    operator.label.startsWith(operatorPrefix),
  ).map((operator, i) => ({
    label: operator.label,
    kind: monaco.languages.CompletionItemKind.Operator,
    detail: operator.detail,
    insertText: operator.insertText,
    range,
    sortText: String(i).padStart(3, "0"),
  }));
}

function buildTimestampSuggestions(monaco, token, range) {
  return LQL_TIMESTAMP_KEYWORDS.filter(
    (kw) =>
      kw.label.toLocaleLowerCase().startsWith(token.toLocaleLowerCase()) ||
      kw.insertText.toLocaleLowerCase().startsWith(token.toLocaleLowerCase()),
  ).map((kw, i) => ({
    label: kw.label,
    kind: monaco.languages.CompletionItemKind.Keyword,
    detail: kw.detail,
    insertText: kw.insertText,
    range,
    sortText: String(i).padStart(3, "0"),
  }));
}

function getMetadataSuggestionSegments(fields, typedPath) {
  const metadataFields = fields.filter((f) => f.name.startsWith("metadata."));
  const pathPrefix = typedPath.includes(".")
    ? typedPath.substring(0, typedPath.lastIndexOf(".") + 1)
    : "";
  const fullPrefix = "metadata." + pathPrefix;
  const seen = new Set();
  const suggestions = [];

  for (const field of metadataFields) {
    if (!field.name.startsWith(fullPrefix)) continue;

    const remainder = field.name.slice(fullPrefix.length);
    if (!remainder) continue;

    const dotIdx = remainder.indexOf(".");
    const segment = dotIdx >= 0 ? remainder.substring(0, dotIdx) : remainder;

    if (seen.has(segment)) continue;
    seen.add(segment);
    suggestions.push(segment);
  }

  return suggestions;
}

const completionKindStub = { Keyword: "keyword", Operator: "operator" };
const completionItemInsertTextRuleStub = { InsertAsSnippet: "InsertAsSnippet" };

export function hasLqlSuggestions(
  textUntilPosition,
  fullLine,
  schemaFields,
  suggestedSearches,
) {
  const operatorMatch = textUntilPosition.match(
    /(?:^|\s)-?(?:(?:m|metadata)\.[\w.]+|(?:t|timestamp)|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*):([@><~=]*)$/,
  );
  const timestampValueMatch = textUntilPosition.match(
    /(?:^|\s)(?:t|timestamp):([a-zA-Z@]*)$/,
  );
  const tokenMatch = textUntilPosition.match(/(?:^|\s)(\S+)$/);
  const currentToken = tokenMatch ? tokenMatch[1] : null;
  const metaMatch = textUntilPosition.match(
    /(?:^|[\s:])(?:m|metadata)\.([\w.]*?)$/,
  );

  if (timestampValueMatch) {
    return (
      buildTimestampSuggestions(
        {
          languages: {
            CompletionItemKind: completionKindStub,
          },
        },
        timestampValueMatch[1],
        null,
      ).length > 0
    );
  }

  if (metaMatch) {
    return getMetadataSuggestionSegments(schemaFields, metaMatch[1]).length > 0;
  }

  if (operatorMatch) {
    return (
      buildOperatorSuggestions(
        {
          languages: {
            CompletionItemKind: completionKindStub,
          },
        },
        `:${operatorMatch[1]}`,
        null,
      ).length > 0
    );
  }

  if (
    currentToken &&
    textUntilPosition.length > 0 &&
    /^\S+$/.test(textUntilPosition) &&
    fullLine === textUntilPosition &&
    !["t:", "timestamp:"].includes(currentToken)
  ) {
    const keywordSuggestions = buildKeywordSuggestions(
      {
        languages: {
          CompletionItemKind: completionKindStub,
          CompletionItemInsertTextRule: completionItemInsertTextRuleStub,
        },
      },
      currentToken,
      null,
    );
    const hasSavedSearchMatches = [...new Set(suggestedSearches)].some(
      (querystring) =>
        querystring
          .toLocaleLowerCase()
          .startsWith(currentToken.toLocaleLowerCase()),
    );

    return (
      hasSavedSearchMatches ||
      (keywordSuggestions.length > 0 &&
        !hasSingleExactKeywordSuggestion(currentToken, keywordSuggestions))
    );
  }

  if (/(?:^|[\s])$/.test(textUntilPosition)) {
    return true;
  }

  if (currentToken) {
    const keywordSuggestions = buildKeywordSuggestions(
      {
        languages: {
          CompletionItemKind: completionKindStub,
          CompletionItemInsertTextRule: completionItemInsertTextRuleStub,
        },
      },
      currentToken,
      null,
    );

    return (
      keywordSuggestions.length > 0 &&
      !hasSingleExactKeywordSuggestion(currentToken, keywordSuggestions)
    );
  }

  return false;
}

/**
 * Registers a CompletionItemProvider for the LQL language.
 * @param {object} monaco - The monaco-editor namespace
 * @param {function} getFields - Returns current schema fields array [{name, type}]
 * @param {function} getSuggestedSearches - Returns saved-search suggestions [querystring]
 * @returns {IDisposable} The disposable for the registered provider
 */
export function registerLqlCompletionProvider(
  monaco,
  getFields,
  getSuggestedSearches,
) {
  return monaco.languages.registerCompletionItemProvider("lql", {
    triggerCharacters: [".", ":", "@", ">", "<", "~"],

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
      const lineRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: 1,
        endColumn: model.getLineMaxColumn(position.lineNumber),
      };
      const operatorMatch = textUntilPosition.match(
        /(?:^|\s)-?(?:(?:m|metadata)\.[\w.]+|(?:t|timestamp)|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*):([@><~=]*)$/,
      );
      const timestampValueMatch = textUntilPosition.match(
        /(?:^|\s)(?:t|timestamp):([a-zA-Z@]*)$/,
      );
      const fullLine = model.getLineContent(position.lineNumber);
      const tokenMatch = textUntilPosition.match(/(?:^|\s)(\S+)$/);
      const currentToken = tokenMatch ? tokenMatch[1] : null;
      const currentTokenRange = currentToken
        ? {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: position.column - currentToken.length,
            endColumn: position.column,
          }
        : replaceRange;
      const savedSearchQuerystrings = [...new Set(getSuggestedSearches())];
      const buildSavedSearchSuggestions = (querystrings, range) =>
        querystrings.map((querystring, index) => ({
          label: querystring,
          kind: monaco.languages.CompletionItemKind.Snippet,
          detail: "saved search",
          insertText: querystring,
          range,
          sortText: `0-${String(index).padStart(3, "0")}`,
        }));

      if (timestampValueMatch) {
        const timestampToken = timestampValueMatch[1];
        const suggestions = buildTimestampSuggestions(monaco, timestampToken, {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: position.column - timestampToken.length,
          endColumn: position.column,
        });

        if (suggestions.length > 0) {
          return { suggestions };
        }
      }

      if (operatorMatch) {
        const operatorPrefix = `:${operatorMatch[1]}`;
        const operatorRange = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: position.column - operatorPrefix.length,
          endColumn: position.column,
        };
        const suggestions = buildOperatorSuggestions(
          monaco,
          operatorPrefix,
          operatorRange,
        );

        if (suggestions.length > 0) {
          return { suggestions };
        }
      }

      if (
        currentToken &&
        textUntilPosition.length > 0 &&
        /^\S+$/.test(textUntilPosition) &&
        fullLine === textUntilPosition &&
        !["t:", "timestamp:"].includes(currentToken)
      ) {
        const matchedSavedSearches = savedSearchQuerystrings.filter(
          (querystring) =>
            querystring
              .toLocaleLowerCase()
              .startsWith(currentToken.toLocaleLowerCase()),
        );
        const filteredSavedSearchQuerystrings =
          matchedSavedSearches.length === 1 &&
          matchedSavedSearches[0].toLocaleLowerCase() ===
            currentToken.toLocaleLowerCase()
            ? []
            : matchedSavedSearches;
        const savedSearchSuggestions = buildSavedSearchSuggestions(
          filteredSavedSearchQuerystrings,
          lineRange,
        );

        const keywordSuggestions = buildKeywordSuggestions(
          monaco,
          currentToken,
          lineRange,
        );
        const filteredKeywordSuggestions = hasSingleExactKeywordSuggestion(
          currentToken,
          keywordSuggestions,
        )
          ? []
          : keywordSuggestions;
        const sortedKeywordSuggestions = filteredKeywordSuggestions.map(
          (suggestion, index) => ({
            ...suggestion,
            sortText: `1-${String(index).padStart(3, "0")}`,
          }),
        );

        const suggestions = [
          ...savedSearchSuggestions,
          ...sortedKeywordSuggestions,
        ];

        if (suggestions.length > 0) {
          return { suggestions };
        }
      }

      // Check if we're in a metadata path context: m. or metadata. prefix
      const metaMatch = textUntilPosition.match(
        /(?:^|[\s:])(?:m|metadata)\.([\w.]*?)$/,
      );

      if (metaMatch) {
        const typedPath = metaMatch[1]; // e.g. "request." or "req"
        const fields = getFields();
        const suggestions = [];
        const metadataFields = fields.filter((f) =>
          f.name.startsWith("metadata."),
        );
        const pathPrefix = typedPath.includes(".")
          ? typedPath.substring(0, typedPath.lastIndexOf(".") + 1)
          : "";
        const fullPrefix = "metadata." + pathPrefix;

        for (const segment of getMetadataSuggestionSegments(
          fields,
          typedPath,
        )) {
          const field = metadataFields.find(
            (item) =>
              item.name === `${fullPrefix}${segment}` ||
              item.name.startsWith(`${fullPrefix}${segment}.`),
          );
          const isLeaf = field
            ? field.name === `${fullPrefix}${segment}`
            : true;

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
        const keywordSuggestions = LQL_KEYWORDS.map((kw, i) => ({
          label: kw.label,
          kind: monaco.languages.CompletionItemKind.Keyword,
          detail: kw.detail,
          insertText: kw.insertText,
          insertTextRules: kw.insertText.includes("$0")
            ? monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
            : undefined,
          range: replaceRange,
          sortText: `1-${String(i).padStart(3, "0")}`,
        }));
        const showSavedSearches = textUntilPosition.trim().length === 0;
        const savedSearchSuggestions = showSavedSearches
          ? buildSavedSearchSuggestions(savedSearchQuerystrings, lineRange)
          : [];
        const suggestions = [...savedSearchSuggestions, ...keywordSuggestions];

        return { suggestions };
      }

      if (currentToken) {
        const suggestions = buildKeywordSuggestions(
          monaco,
          currentToken,
          currentTokenRange,
        );

        if (!hasSingleExactKeywordSuggestion(currentToken, suggestions)) {
          return { suggestions };
        }
      }

      return { suggestions: [] };
    },
  });
}
