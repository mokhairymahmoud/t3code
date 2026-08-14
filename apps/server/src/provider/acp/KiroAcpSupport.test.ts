import { describe, expect, it } from "@effect/vitest";

import { buildKiroAcpSpawnInput, resolveKiroAcpBaseModelId } from "./KiroAcpSupport.ts";

describe("resolveKiroAcpBaseModelId", () => {
  it("falls back to Kiro's auto-select sentinel", () => {
    expect(resolveKiroAcpBaseModelId(undefined)).toBe("auto");
    expect(resolveKiroAcpBaseModelId(null)).toBe("auto");
    expect(resolveKiroAcpBaseModelId("   ")).toBe("auto");
  });

  it("trims explicit model slugs", () => {
    expect(resolveKiroAcpBaseModelId("  claude-sonnet-4.5  ")).toBe("claude-sonnet-4.5");
  });
});

describe("buildKiroAcpSpawnInput", () => {
  it("spawns the CLI's ACP server", () => {
    const spawn = buildKiroAcpSpawnInput(
      { binaryPath: "/usr/local/bin/kiro-cli", agent: "" },
      "/tmp/project",
      { KIRO_API_KEY: "test-key" },
    );

    expect(spawn).toEqual({
      command: "/usr/local/bin/kiro-cli",
      args: ["acp"],
      cwd: "/tmp/project",
      env: { KIRO_API_KEY: "test-key" },
    });
  });

  it("defaults to the `kiro-cli` binary on PATH", () => {
    expect(buildKiroAcpSpawnInput(null, "/tmp/project").command).toBe("kiro-cli");
    expect(buildKiroAcpSpawnInput({ binaryPath: "", agent: "" }, "/tmp/project").command).toBe(
      "kiro-cli",
    );
  });

  it("passes an explicit model through --model", () => {
    expect(buildKiroAcpSpawnInput(null, "/tmp/project", undefined, "gpt-5.4").args).toEqual([
      "acp",
      "--model",
      "gpt-5.4",
    ]);
  });

  it("passes an optional Kiro agent through --agent", () => {
    expect(
      buildKiroAcpSpawnInput({ binaryPath: "kiro-cli", agent: "reviewer" }, "/tmp/project").args,
    ).toEqual(["acp", "--agent", "reviewer"]);
  });

  it("omits --model when the model is auto, so Kiro keeps its own default", () => {
    // Kiro treats a missing `--model` and `--model auto` the same way, and
    // leaving the flag off keeps the spawn line closer to what users run.
    expect(buildKiroAcpSpawnInput(null, "/tmp/project", undefined, "auto").args).toEqual(["acp"]);
    expect(buildKiroAcpSpawnInput(null, "/tmp/project", undefined, "  ").args).toEqual(["acp"]);
  });
});
