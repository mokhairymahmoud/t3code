import { describe, expect, it } from "@effect/vitest";

import {
  buildGithubCopilotAcpSpawnInput,
  resolveGithubCopilotAcpBaseModelId,
} from "./GithubCopilotAcpSupport.ts";

describe("resolveGithubCopilotAcpBaseModelId", () => {
  it("falls back to Copilot's own auto-select sentinel", () => {
    expect(resolveGithubCopilotAcpBaseModelId(undefined)).toBe("auto");
    expect(resolveGithubCopilotAcpBaseModelId(null)).toBe("auto");
    expect(resolveGithubCopilotAcpBaseModelId("   ")).toBe("auto");
  });

  it("trims explicit model slugs", () => {
    expect(resolveGithubCopilotAcpBaseModelId("  claude-sonnet-4.5  ")).toBe("claude-sonnet-4.5");
  });
});

describe("buildGithubCopilotAcpSpawnInput", () => {
  it("spawns the CLI's ACP server", () => {
    const spawn = buildGithubCopilotAcpSpawnInput(
      { binaryPath: "/usr/local/bin/copilot" },
      "/tmp/project",
      { GH_TOKEN: "secret" },
    );

    expect(spawn).toEqual({
      command: "/usr/local/bin/copilot",
      args: ["--acp"],
      cwd: "/tmp/project",
      env: { GH_TOKEN: "secret" },
    });
  });

  it("defaults to the `copilot` binary on PATH", () => {
    expect(buildGithubCopilotAcpSpawnInput(null, "/tmp/project").command).toBe("copilot");
    expect(buildGithubCopilotAcpSpawnInput({ binaryPath: "" }, "/tmp/project").command).toBe(
      "copilot",
    );
  });

  it("passes an explicit model through --model", () => {
    expect(
      buildGithubCopilotAcpSpawnInput(null, "/tmp/project", undefined, "gpt-5.4").args,
    ).toEqual(["--acp", "--model", "gpt-5.4"]);
  });

  it("omits --model when the model is auto, so Copilot keeps its own default", () => {
    // Copilot treats a missing `--model` and `--model auto` the same way, and
    // leaving the flag off keeps the spawn line closer to what users run.
    expect(buildGithubCopilotAcpSpawnInput(null, "/tmp/project", undefined, "auto").args).toEqual([
      "--acp",
    ]);
    expect(buildGithubCopilotAcpSpawnInput(null, "/tmp/project", undefined, "  ").args).toEqual([
      "--acp",
    ]);
  });
});
