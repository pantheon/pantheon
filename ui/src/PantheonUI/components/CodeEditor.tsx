import { styled, useWindowSize } from "@operational/components";
import React, { useEffect, useRef, useState } from "react";

import createCodeEditor, { Config, Editor } from "@contiamo/code-editor";
import { Tree } from "@contiamo/code-editor/lib/psl/psl-treeAnalyser";
import Loader from "./Loader";

export interface CodeEditorProps {
  label?: string;
  value: string;
  language?: Config["language"];
  onChange?: (newVal: string) => void;
  onSubmit?: () => void;
  onPslTreeChange?: (tree: Tree | {}) => void;
  getPslTables?: Config["getTables"];
  getPslDataSources?: Config["getDataSources"];
  height: number | ((windowHeight: number) => number);
  disabled?: boolean;
  codeEditorRef?: (editor: Editor) => void;
}

const noForward = { shouldForwardProp: (props: string) => !["height", "ref"].includes(props) };

const EditorNode = styled("div", noForward)<{ height?: number | string; disabled?: boolean }>(props => ({
  height: props.height,
  opacity: props.disabled ? 0.8 : 1,
  display: "block",
  boxSizing: "border-box",
  overflow: "hidden",
  ".scroll-decoration": {
    boxShadow: "none !important",
  },
  ...(props.disabled
    ? {
        ".line-numbers": {
          color: `${props.theme.color.text.lightest} !important`,
        },
      }
    : {}),
}));

const CodeEditor: React.SFC<CodeEditorProps> = props => {
  const $editorNode = useRef<HTMLDivElement | null>(null);
  const $container = useRef<HTMLDivElement>(null);
  const editor = useRef<Editor | null>(null);
  const { height: windowHeight, width: windowWidth } = useWindowSize();
  const [isLoading, setIsLoading] = useState(true);

  // Init monaco editor
  useEffect(() => {
    if ($editorNode.current) {
      // Create the editor
      editor.current = createCodeEditor($editorNode.current, {
        language: props.language || "psl",
        value: props.value,
        disabled: Boolean(props.disabled),
        getTables: props.getPslTables,
        getDataSources: props.getPslDataSources,
        onSubmit: props.onSubmit,
      });

      // Initial loading done
      setIsLoading(false);

      // Attach the ref
      if (props.codeEditorRef) {
        props.codeEditorRef(editor.current);
      }

      // Init the PSL tree
      if (props.onPslTreeChange) {
        props.onPslTreeChange(editor.current.tree);
      }
    }

    return function unmountMonacoEditor() {
      if (editor.current) {
        editor.current.unmount();
      }
    };
  }, []);

  // Watch for `disabled` props (readonly mode)
  useEffect(
    () => {
      if (editor.current) {
        if (props.disabled) {
          editor.current.disable();
        } else {
          editor.current.enable();
        }
      }
    },
    [props.disabled],
  );

  // Subscribe for code-editor event
  useEffect(() => {
    if (editor.current) {
      editor.current.subscribe((value, tree) => {
        if (props.onChange) {
          props.onChange(value);
        }
        if (props.onPslTreeChange) {
          props.onPslTreeChange(tree);
        }
      });
    }

    return () => {
      if (editor.current) {
        editor.current.unSubscribeAll();
      }
    };
  }, []);

  // Watch for `value` prop
  useEffect(
    () => {
      if (editor.current && props.value !== editor.current.getValue()) {
        editor.current.setValue(props.value);
      }
    },
    [props.value],
  );

  // Calculate the height
  const height = typeof props.height === "function" ? props.height(windowHeight) : props.height;

  // Watch window resize
  useEffect(
    () => {
      if (editor.current && $container.current) {
        editor.current.layout({ width: 0, height: 0 }); // Ensure to have a correct container width calcul bellow (otherwise it doesn't shrink)
        const { width } = $container.current.getBoundingClientRect();
        editor.current.layout({ width, height });
      }
    },
    [windowWidth],
  );

  return (
    <div style={{ height, position: "relative" }} ref={$container}>
      {isLoading && <Loader />}
      <EditorNode disabled={props.disabled} height={height} ref={$editorNode} />
    </div>
  );
};

export default CodeEditor;

export { Editor };
