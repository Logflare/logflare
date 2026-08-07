import { describe, expect, it } from "vitest";

const { collectTrimmedFields, trimFieldInput } = await import(
  "../lql_editor_wrapper_hook.js"
);

const input = (name, value) => ({ name, value });

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
