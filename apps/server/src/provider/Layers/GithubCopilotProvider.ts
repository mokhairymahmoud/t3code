/**
 * GithubCopilotProvider — health probe and snapshot for the GitHub Copilot CLI.
 *
 * The probe is a plain `copilot --version` call. Unlike Cursor and Grok there
 * is no model-discovery round trip: Copilot's ACP `session/new` carries no
 * `SessionModelState`, so the catalog is `auto` plus whatever the user lists
 * under `customModels`.
 *
 * @module provider/Layers/GithubCopilotProvider
 */
import {
  type GithubCopilotSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { GITHUB_COPILOT_AUTO_MODEL } from "../acp/GithubCopilotAcpSupport.ts";

const GITHUB_COPILOT_PRESENTATION = {
  displayName: "GitHub Copilot",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  // No `session/set_model` on Copilot's ACP surface — the model is fixed by
  // the `--model` flag the session was spawned with.
  requiresNewThreadForModelChange: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;

const GITHUB_COPILOT_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: GITHUB_COPILOT_AUTO_MODEL,
    name: "Auto",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function githubCopilotModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    GITHUB_COPILOT_BUILT_IN_MODELS,
    customModels ?? [],
    EMPTY_CAPABILITIES,
  );
}

export function buildInitialGithubCopilotProviderSnapshot(
  githubCopilotSettings: GithubCopilotSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = githubCopilotModelsFromSettings(githubCopilotSettings.customModels);

    if (!githubCopilotSettings.enabled) {
      return buildServerProvider({
        presentation: GITHUB_COPILOT_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "GitHub Copilot is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: GITHUB_COPILOT_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking GitHub Copilot CLI availability...",
      },
    });
  });
}

const runGithubCopilotVersionCommand = (
  githubCopilotSettings: GithubCopilotSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = githubCopilotSettings.binaryPath || "copilot";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export const checkGithubCopilotProviderStatus = Effect.fn("checkGithubCopilotProviderStatus")(
  function* (
    githubCopilotSettings: GithubCopilotSettings,
    environment: NodeJS.ProcessEnv = process.env,
  ): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = githubCopilotModelsFromSettings(githubCopilotSettings.customModels);

    if (!githubCopilotSettings.enabled) {
      return buildServerProvider({
        presentation: GITHUB_COPILOT_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "GitHub Copilot is disabled in T3 Code settings.",
        },
      });
    }

    const versionResult = yield* runGithubCopilotVersionCommand(
      githubCopilotSettings,
      environment,
    ).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.result);

    if (Result.isFailure(versionResult)) {
      const error = versionResult.failure;
      yield* Effect.logWarning("GitHub Copilot CLI health check failed.", {
        errorTag: error._tag,
      });
      return buildServerProvider({
        presentation: GITHUB_COPILOT_PRESENTATION,
        enabled: githubCopilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "GitHub Copilot CLI (`copilot`) is not installed or not on PATH."
            : "Failed to execute GitHub Copilot CLI health check.",
        },
      });
    }

    if (Option.isNone(versionResult.success)) {
      return buildServerProvider({
        presentation: GITHUB_COPILOT_PRESENTATION,
        enabled: githubCopilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message:
            "GitHub Copilot CLI is installed but timed out while running `copilot --version`.",
        },
      });
    }

    const versionOutput = versionResult.success.value;
    const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
    if (versionOutput.code !== 0) {
      yield* Effect.logWarning("GitHub Copilot CLI version probe exited with a non-zero status.", {
        exitCode: versionOutput.code,
        stdoutLength: versionOutput.stdout.length,
        stderrLength: versionOutput.stderr.length,
      });
      return buildServerProvider({
        presentation: GITHUB_COPILOT_PRESENTATION,
        enabled: githubCopilotSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version,
          status: "error",
          auth: { status: "unknown" },
          message: "GitHub Copilot CLI is installed but failed to run.",
        },
      });
    }

    return buildServerProvider({
      presentation: GITHUB_COPILOT_PRESENTATION,
      enabled: githubCopilotSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
      },
    });
  },
);

export const enrichGithubCopilotSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("GitHub Copilot version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
