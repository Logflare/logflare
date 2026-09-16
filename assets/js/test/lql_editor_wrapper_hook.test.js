import { describe, expect, it, vi } from "vitest";

const {
  collectTrimmedFields,
  trimFieldInput,
  default: LqlEditorWrapper,
} = await import("../lql_editor_wrapper_hook.js");

const input = (name, value) => ({ name, value });

const mountHook = () => {
  const container = {
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
  };

  const hook = Object.create(LqlEditorWrapper);

  hook.el = {
    dataset: {},
    addEventListener: vi.fn(),
    closest: () => container,
    parentElement: null,
  };

  hook.mounted();

  return { container, hook };
};

describe("trimFieldInput", () => {
  it("trims the input value in place", () => {
    const field = input("fields[metadata.project_ref]", "  abcdef  ");

    expect(trimFieldInput(field)).toEqual({
      name: "metadata.project_ref",
      value: "abcdef",
    });
    expect(field.value).toBe("abcdef");
  });

  it("trims tabs and newlines", () => {
    const field = input("fields[metadata.level]", "\terror\n");

    trimFieldInput(field);

    expect(field.value).toBe("error");
  });

  it("returns null and leaves unrelated inputs alone", () => {
    const field = input("querystring", "  event_message:timeout  ");

    expect(trimFieldInput(field)).toBeNull();
    expect(field.value).toBe("  event_message:timeout  ");
  });
});

describe("collectTrimmedFields", () => {
  it("collects trimmed values keyed by field path", () => {
    const fields = collectTrimmedFields([
      input("fields[metadata.organization_slug]", " abcdef "),
      input("fields[metadata.project_ref]", "ghijkl"),
      input("fields[metadata.request_id]", "   "),
      input("tailing?", " true "),
    ]);

    expect(fields).toEqual({
      "metadata.organization_slug": "abcdef",
      "metadata.project_ref": "ghijkl",
      "metadata.request_id": "",
    });
  });

  it("returns an empty map when there are no inputs", () => {
    expect(collectTrimmedFields([])).toEqual({});
  });
});

describe("focusout handler lifecycle", () => {
  it("registers a focusout handler on the search control when mounted", () => {
    const { container } = mountHook();

    expect(container.addEventListener).toHaveBeenCalledWith(
      "focusout",
      expect.any(Function)
    );
  });

  it("trims the focused out field via the registered handler", () => {
    const { container } = mountHook();
    const [, handler] = container.addEventListener.mock.calls[0];
    const field = input("fields[metadata.project_ref]", "  abcdef  ");

    handler({ target: field });

    expect(field.value).toBe("abcdef");
  });

  it("removes the same handler when destroyed", () => {
    const { container, hook } = mountHook();
    const [, handler] = container.addEventListener.mock.calls[0];

    hook.destroyed();

    expect(container.removeEventListener).toHaveBeenCalledWith(
      "focusout",
      handler
    );
  });

  it("does not blow up when destroyed without a search control", () => {
    const hook = Object.create(LqlEditorWrapper);

    hook.el = {
      dataset: {},
      addEventListener: vi.fn(),
      closest: () => null,
      parentElement: null,
    };

    hook.mounted();

    expect(() => hook.destroyed()).not.toThrow();
  });
});

describe("editor keyboard handling", () => {
  it("stops Monaco from consuming Tab when autocomplete suggestions are hidden", () => {
    const { hook } = mountHook();
    const event = {
      key: "Tab",
      preventDefault: vi.fn(),
      stopImmediatePropagation: vi.fn(),
    };

    hook._handleEditorKeydown(event);

    expect(event.stopImmediatePropagation).toHaveBeenCalledOnce();
    expect(event.preventDefault).not.toHaveBeenCalled();
  });

  it("leaves Tab to Monaco when autocomplete suggestions are visible", () => {
    const { hook } = mountHook();
    hook._editorDomNode = {
      querySelector: vi.fn(() => ({ className: "suggest-widget visible" })),
    };
    const event = {
      key: "Tab",
      stopImmediatePropagation: vi.fn(),
    };

    hook._handleEditorKeydown(event);

    expect(hook._editorDomNode.querySelector).toHaveBeenCalledWith(
      ".suggest-widget.visible"
    );
    expect(event.stopImmediatePropagation).not.toHaveBeenCalled();
  });

  it("leaves other keys to Monaco", () => {
    const { hook } = mountHook();
    const event = {
      key: "Enter",
      stopImmediatePropagation: vi.fn(),
    };

    hook._handleEditorKeydown(event);

    expect(event.stopImmediatePropagation).not.toHaveBeenCalled();
  });
});
