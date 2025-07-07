/*
Listen for editor mount and register completion provider
Supports MonacoEditorComponent
*/
const editorMountedHandler = (ev) => {
  // build a list of completions for the editor
  const dataCompletionsName = ev.detail.hook.el.getAttribute(
    "data-completions-name",
  );
  const completionsEl = document.querySelector(
    `[name="${dataCompletionsName}"]`,
  );

  let completions = [];

  if (completionsEl != null) {
    completions = JSON.parse(completionsEl.value);
  }

  const editor = ev.detail.editor.standalone_code_editor;

  function createDependencyProposals(range) {
    return completions.map(function (name) {
      return {
        label: name,
        kind: monaco.languages.CompletionItemKind.Module,
        insertText: name,
        range: range,
      };
    });
  }

  monaco.languages.registerCompletionItemProvider("sql", {
    provideCompletionItems: function (model, position) {
      var word = model.getWordUntilPosition(position);
      var range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };
      return {
        suggestions: createDependencyProposals(range),
      };
    },
  });
};

export default editorMountedHandler;
