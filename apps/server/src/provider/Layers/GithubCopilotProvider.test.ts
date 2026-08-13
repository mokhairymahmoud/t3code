import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { GithubCopilotSettings } from "@t3tools/contracts";

import {
  buildInitialGithubCopilotProviderSnapshot,
  checkGithubCopilotProviderStatus,
} from "./GithubCopilotProvider.ts";

const decodeGithubCopilotSettings = Schema.decodeSync(GithubCopilotSettings);

describe("buildInitialGithubCopilotProviderSnapshot", () => {
  it.effect("is disabled by default, so an uninstalled CLI never shows as broken", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialGithubCopilotProviderSnapshot(
        decodeGithubCopilotSettings({}),
      );
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.installed).toBe(false);
      expect(snapshot.message).toContain("disabled");
    }),
  );

  it.effect("returns a pending snapshot once enabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialGithubCopilotProviderSnapshot(
        decodeGithubCopilotSettings({ enabled: true }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.version).toBeNull();
      expect(snapshot.message).toContain("Checking GitHub Copilot");
      // Copilot binds its model at spawn, so a model change needs a new thread.
      expect(snapshot.requiresNewThreadForModelChange).toBe(true);
    }),
  );

  it.effect("offers auto plus any user-configured custom models", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialGithubCopilotProviderSnapshot(
        decodeGithubCopilotSettings({ enabled: true, customModels: ["gpt-5.4", "gpt-5.4"] }),
      );
      expect(snapshot.models.map((model) => model.slug)).toEqual(["auto", "gpt-5.4"]);
    }),
  );
});

it.layer(NodeServices.layer)("checkGithubCopilotProviderStatus", (it) => {
  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkGithubCopilotProviderStatus(
        decodeGithubCopilotSettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/copilot-binary",
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("reports an installed CLI as unhealthy when --version exits non-zero", () =>
    Effect.gen(function* () {
      const secretStderr = "broken copilot install: secret-token-value";
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-copilot-version-" });
          const copilotPath = path.join(dir, "copilot");
          yield* fs.writeFileString(
            copilotPath,
            ["#!/bin/sh", `printf "%s\\n" "${secretStderr}" >&2`, "exit 2", ""].join("\n"),
          );
          yield* fs.chmod(copilotPath, 0o755);

          return yield* checkGithubCopilotProviderStatus(
            decodeGithubCopilotSettings({ enabled: true, binaryPath: copilotPath }),
          );
        }),
      );

      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toBe("GitHub Copilot CLI is installed but failed to run.");
      expect(snapshot.message).not.toContain(secretStderr);
    }),
  );

  it.effect("parses the version out of the CLI banner", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-copilot-success-" });
          const copilotPath = path.join(dir, "copilot");
          // Matches the real `copilot --version` output, which prints an
          // update hint on a second line.
          yield* fs.writeFileString(
            copilotPath,
            [
              "#!/bin/sh",
              'printf "GitHub Copilot CLI 1.0.79.\\n"',
              "printf \"Run 'copilot update' to check for updates.\\n\"",
              "exit 0",
              "",
            ].join("\n"),
          );
          yield* fs.chmod(copilotPath, 0o755);

          return yield* checkGithubCopilotProviderStatus(
            decodeGithubCopilotSettings({ enabled: true, binaryPath: copilotPath }),
          );
        }),
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.version).toBe("1.0.79");
      expect(snapshot.models.map((model) => model.slug)).toEqual(["auto"]);
    }),
  );
});
