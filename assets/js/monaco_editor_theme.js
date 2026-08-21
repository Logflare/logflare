// Copied from live_monaco_editor's bundled theme:
// https://github.com/BeaconCMS/live_monaco_editor/blob/c5c11d9f3632ea110f86e7eb0b68e4f48dc2a14e/assets/js/live_monaco_editor/editor/themes.js
// Copyright (c) 2023 DockYard, Inc
// Licensed under MIT license
// live_monaco_editor copied and modified the original work from:
// https://github.com/livebook-dev/livebook/blob/23e58ac604de92ce54472f36fe3e28dc27576d6c/assets/js/hooks/cell_editor/live_editor/theme.js
// Copyright (C) 2021 Dashbit
// Licensed under Apache 2.0 available at https://www.apache.org/licenses/LICENSE-2.0

// This is a port of the One Dark theme to the Monaco editor.
// We color graded the comment so it has AA accessibility and
// then similarly scaled the default font.
const colors = {
  background: "#282c34",
  default: "#c4cad6",
  lightRed: "#e06c75",
  blue: "#61afef",
  gray: "#8c92a3",
  green: "#98c379",
  purple: "#c678dd",
  red: "#be5046",
  teal: "#56b6c2",
  peach: "#d19a66",
};

const rules = (colors) => [
  { token: "", foreground: colors.default },
  { token: "variable", foreground: colors.lightRed },
  { token: "constant", foreground: colors.blue },
  { token: "constant.character.escape", foreground: colors.blue },
  { token: "comment", foreground: colors.gray },
  { token: "number", foreground: colors.blue },
  { token: "regexp", foreground: colors.lightRed },
  { token: "type", foreground: colors.lightRed },
  { token: "string", foreground: colors.green },
  { token: "keyword", foreground: colors.purple },
  { token: "operator", foreground: colors.peach },
  { token: "delimiter.bracket.embed", foreground: colors.red },
  { token: "sigil", foreground: colors.teal },
  { token: "function", foreground: colors.blue },
  { token: "function.call", foreground: colors.default },

  // Markdown specific
  { token: "emphasis", fontStyle: "italic" },
  { token: "strong", fontStyle: "bold" },
  { token: "keyword.md", foreground: colors.lightRed },
  { token: "keyword.table", foreground: colors.lightRed },
  { token: "string.link.md", foreground: colors.blue },
  { token: "variable.md", foreground: colors.teal },
  { token: "string.md", foreground: colors.default },
  { token: "variable.source.md", foreground: colors.default },

  // XML specific
  { token: "tag", foreground: colors.lightRed },
  { token: "metatag", foreground: colors.lightRed },
  { token: "attribute.name", foreground: colors.peach },
  { token: "attribute.value", foreground: colors.green },

  // JSON specific
  { token: "string.key", foreground: colors.lightRed },
  { token: "keyword.json", foreground: colors.blue },

  // SQL specific
  { token: "operator.sql", foreground: colors.purple },
];

const theme = {
  base: "vs-dark",
  inherit: false,
  rules: rules(colors),
  colors: {
    "editor.background": colors.background,
    "editor.foreground": colors.default,
    "editorLineNumber.foreground": "#636d83",
    "editorCursor.foreground": "#636d83",
    "editor.selectionBackground": "#3e4451",
    "editor.findMatchHighlightBackground": "#528bff3d",
    "editorSuggestWidget.background": "#21252b",
    "editorSuggestWidget.border": "#181a1f",
    "editorSuggestWidget.selectedBackground": "#2c313a",
    "input.background": "#1b1d23",
    "input.border": "#181a1f",
    "editorBracketMatch.border": "#282c34",
    "editorBracketMatch.background": "#3e4451",
  },
};

export { theme };
