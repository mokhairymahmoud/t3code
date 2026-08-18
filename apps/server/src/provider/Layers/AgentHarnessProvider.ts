/** @module provider/Layers/AgentHarnessProvider */
import {
  type AgentHarnessSettings,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { createModelCapabilities } from "@t3tools/shared/model";

import {
  buildServerProvider,
  isCommandMissingCause,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import type { ProviderMaintenanceCapabilities } from "../providerMaintenance.ts";

const AGENT_HARNESS_PRESENTATION = {
  displayName: "Agent Harness",
  showInteractionModeToggle: false,
} as const;

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const MODELS_PROBE_TIMEOUT_MS = 8_000;

const DEFAULT_MODEL_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });

interface AgentHarnessModelDescriptor {
  readonly slug: string;
  readonly name: string;
  readonly contextWindow?: number;
}

function parseModelsOutput(stdout: string): ReadonlyArray<ServerProviderModel> {
  try {
    const raw = JSON.parse(stdout.trim());
    if (!Array.isArray(raw)) return [];
    return raw
      .filter(
        (entry: unknown): entry is AgentHarnessModelDescriptor =>
          typeof entry === "object" &&
          entry !== null &&
          typeof (entry as Record<string, unknown>).slug === "string" &&
          typeof (entry as Record<string, unknown>).name === "string",
      )
      .map((entry) => ({
        slug: entry.slug,
        name: entry.name,
        isCustom: false,
        capabilities: DEFAULT_MODEL_CAPABILITIES,
      }));
  } catch {
    return [];
  }
}

export function buildInitialAgentHarnessProviderSnapshot(
  settings: AgentHarnessSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return buildServerProvider({
      presentation: AGENT_HARNESS_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: [],
      probe: settings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking Agent Harness availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Agent Harness is disabled in T3 Code settings.",
          },
    });
  });
}

export const checkAgentHarnessProviderStatus = Effect.fn("checkAgentHarnessProviderStatus")(
  function* (
    settings: AgentHarnessSettings,
    environment: NodeJS.ProcessEnv = process.env,
  ): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    if (!settings.enabled) {
      return buildServerProvider({
        presentation: AGENT_HARNESS_PRESENTATION,
        enabled: false,
        checkedAt,
        models: [],
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Agent Harness is disabled in T3 Code settings.",
        },
      });
    }

    const command = settings.binaryPath || "agent-harness";

    // Step 1: Check binary exists via --help
    const helpResult = yield* Effect.gen(function* () {
      const spawn = yield* resolveSpawnCommand(command, ["--help"], { env: environment });
      return yield* spawnAndCollect(
        command,
        ChildProcess.make(spawn.command, spawn.args, { env: environment, shell: spawn.shell }),
      );
    }).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.result);

    if (Result.isFailure(helpResult)) {
      const error = helpResult.failure;
      return buildServerProvider({
        presentation: AGENT_HARNESS_PRESENTATION,
        enabled: true,
        checkedAt,
        models: [],
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "Agent Harness (`agent-harness`) is not installed or not on PATH."
            : "Failed to execute Agent Harness health check.",
        },
      });
    }
    if (Option.isNone(helpResult.success)) {
      return buildServerProvider({
        presentation: AGENT_HARNESS_PRESENTATION,
        enabled: true,
        checkedAt,
        models: [],
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Agent Harness timed out while running health check.",
        },
      });
    }

    const helpOutput = helpResult.success.value;
    const binaryPresent = helpOutput.code === 0 || helpOutput.code === 2;
    if (!binaryPresent) {
      return buildServerProvider({
        presentation: AGENT_HARNESS_PRESENTATION,
        enabled: true,
        checkedAt,
        models: [],
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "Agent Harness is installed but failed to run.",
        },
      });
    }

    // Step 2: Discover models via `agent-harness models`
    const modelsResult = yield* Effect.gen(function* () {
      const spawn = yield* resolveSpawnCommand(command, ["models"], { env: environment });
      return yield* spawnAndCollect(
        command,
        ChildProcess.make(spawn.command, spawn.args, { env: environment, shell: spawn.shell }),
      );
    }).pipe(Effect.timeoutOption(MODELS_PROBE_TIMEOUT_MS), Effect.result);

    let models: ReadonlyArray<ServerProviderModel> = [];
    if (Result.isSuccess(modelsResult) && Option.isSome(modelsResult.success)) {
      const output = modelsResult.success.value;
      if (output.code === 0) {
        models = parseModelsOutput(output.stdout);
      }
    }

    return buildServerProvider({
      presentation: AGENT_HARNESS_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "ready",
        auth: { status: "unknown" },
        message:
          models.length > 0
            ? `Agent Harness is ready with ${models.length} model${models.length === 1 ? "" : "s"}.`
            : "Agent Harness is ready.",
      },
    });
  },
);

export const enrichAgentHarnessSnapshot = (_input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<void> => Effect.void;
