import { describe, expect, it } from "@effect/vitest";

import {
  DEFAULT_AGENT_HARNESS_BINARY,
  resolveAgentHarnessBinaryPath,
} from "./agentHarnessCommand.ts";

describe("resolveAgentHarnessBinaryPath", () => {
  it("routes the legacy bare command to the Rust binary", () => {
    expect(resolveAgentHarnessBinaryPath("agent-harness")).toBe(DEFAULT_AGENT_HARNESS_BINARY);
  });

  it("preserves explicit paths and commands", () => {
    expect(resolveAgentHarnessBinaryPath("/tmp/custom-agent-harness")).toBe(
      "/tmp/custom-agent-harness",
    );
    expect(resolveAgentHarnessBinaryPath("  agent-harness-rs  ")).toBe("agent-harness-rs");
  });
});
