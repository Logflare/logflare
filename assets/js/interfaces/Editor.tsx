import { FC, useEffect, useRef } from "react";
import Editor, { EditorProps } from "@monaco-editor/react";

interface Props {
  id: string;
  defaultValue?: string;
  isReadOnly?: boolean;
  onInputChange: string;
  onInputRun?: (value: string) => void;
  hideLineNumbers?: boolean;
  className?: string;
  loading?: boolean;
  options?: EditorProps["options"];
  value?: string;
  darkMode?: boolean;
  autoFocus?: boolean;
  pushEvent: (event: string, payload: Object) => void;
}

const MonacoEditor: FC<Props> = ({
  id,
  defaultValue,
  isReadOnly = false,
  hideLineNumbers = true,
  onInputChange,
  onInputRun = () => {},
  className,
  loading,
  options,
  value,
  darkMode,
  autoFocus,
  pushEvent,
}) => {
  const editorRef = useRef();

  useEffect(() => {
    if (editorRef.current) {
      // alignEditor(editorRef.current)
    }
  }, [id]);

  const setEditorTheme = async (monaco: any) => {
    const theme = {
      base: darkMode ? "vs-dark" : "vs-light", // can also be vs-dark or hc-black
      inherit: true, // can also be false to completely replace the builtin rules
      colors: {
        // "editor.background": "0F172A",
      },
      rules: [
        { background: "0F172A" },
        { token: "", foreground: "D4D4D4" },
        { token: "string.sql", foreground: "24B47E" },
        { token: "comment", foreground: "666666" },
        { token: "predefined.sql", foreground: "D4D4D4" },
        { token: "", foreground: "ffcc00" }, // Trying to figure out how to change the border color of the row selected
      ],
    };
    await monaco.editor.defineTheme("promptpro", theme);
  };
  const onMount = async (editor: any, monaco: any) => {
    editor.addAction({
      label: "Run Query",
      keybindings: [monaco.KeyMod.CtrlCmd + monaco.KeyCode.Enter],
      contextMenuGroupId: "operation",
      contextMenuOrder: 0,
      run: async () => {
        const selectedValue = (editorRef?.current as any)
          .getModel()
          .getValueInRange((editorRef?.current as any)?.getSelection());
        onInputRun(selectedValue || (editorRef?.current as any)?.getValue());
      },
    });

    setTimeout(() => {
      if (autoFocus) {
        editor?.focus();
      }
      editorRef.current = editor;
    }, 500);
  };

  return (
    <Editor
      beforeMount={setEditorTheme}
      value={value ?? undefined}
      path={id}
      className={`monaco-editor ${className} tw-min-h-[14rem] tw-pt-2 tw-px-1 tw-cursor-text`}
      defaultLanguage={"text"}
      defaultValue={defaultValue ?? undefined}
      theme={darkMode ? "vs-dark" : "vs-light"}
      options={{
        tabSize: 2,
        fontSize: 13,
        readOnly: isReadOnly,
        minimap: { enabled: false },
        wordWrap: "on",
        fixedOverflowWidgets: false,
        contextmenu: false,
        quickSuggestions: false,
        hideCursorInOverviewRuler: true,
        smoothScrolling: true,
        scrollbar: {
          vertical: "auto",
          horizontal: "hidden",
          verticalScrollbarSize: 6,
        },
        lineNumbers: hideLineNumbers ? "off" : undefined,
        glyphMargin: hideLineNumbers ? false : undefined,
        lineNumbersMinChars: hideLineNumbers ? 0 : undefined,
        folding: hideLineNumbers ? false : undefined,
      }}
      onMount={onMount}
      onChange={(value) => {
        if (onInputChange) {
          pushEvent(onInputChange, { value });
        }
      }}
    />
  );
};

export default MonacoEditor;
