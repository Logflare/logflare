import { describe, expect, it, vi } from "vitest";

vi.stubGlobal("window", {});
vi.mock("../socket", () => ({ default: { channel: vi.fn() } }));
vi.mock("../user-config-storage", () => ({
  useLocalTime: vi.fn().mockResolvedValue(false),
  flipUseLocalTime: vi.fn(),
}));
vi.mock("../formatters", () => ({
  userSelectedFormatter: vi.fn().mockResolvedValue(() => "formatted-ts"),
}));
vi.mock("../utils", () => ({ activateClipboardForSelector: vi.fn() }));
vi.mock("../logs", () => ({ applyToAllLogTimestamps: vi.fn() }));

const { logTemplate } = await import("../source.js");

const baseEvent = (body) => ({ via_rule_id: null, source_uuid: null, body });

describe("logTemplate log level extraction", () => {
  it("uses top-level body.level when present", async () => {
    const html = await logTemplate(
      baseEvent({ timestamp: 1, event_message: "msg", level: "info" }),
    );
    expect(html).toContain('<mark class="log-level-info">info</mark>');
  });

  it("falls back to body.metadata.level when no top-level level", async () => {
    const html = await logTemplate(
      baseEvent({
        timestamp: 1,
        event_message: "msg",
        metadata: { level: "warning" },
      }),
    );
    expect(html).toContain('<mark class="log-level-warning">warning</mark>');
  });

  it("prefers top-level body.level over body.metadata.level", async () => {
    const html = await logTemplate(
      baseEvent({
        timestamp: 1,
        event_message: "msg",
        level: "error",
        metadata: { level: "info" },
      }),
    );
    expect(html).toContain('<mark class="log-level-error">error</mark>');
    expect(html).not.toContain("log-level-info");
  });

  it("renders no log-level mark when level is absent", async () => {
    const html = await logTemplate(
      baseEvent({ timestamp: 1, event_message: "msg" }),
    );
    expect(html).not.toContain("log-level-");
  });
});
