/** @module provider/Layers/KiroProvider */
import {
  type KiroSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { KIRO_AUTO_MODEL } from "../acp/KiroAcpSupport.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";

const KIRO_PRESENTATION = {
  displayName: "Kiro",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });
const VERSION_PROBE_TIMEOUT_MS = 4_000;
const KIRO_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  { slug: KIRO_AUTO_MODEL, name: "Auto", isCustom: false, capabilities: EMPTY_CAPABILITIES },
];

const KiroModelListResponse = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      model_id: Schema.String,
      model_name: Schema.String,
      description: Schema.optional(Schema.String),
    }),
  ),
});
const decodeKiroModelListResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(KiroModelListResponse),
);

const modelsFromSettings = (settings: KiroSettings) =>
  providerModelsFromSettings(KIRO_BUILT_IN_MODELS, settings.customModels, EMPTY_CAPABILITIES);

export function buildInitialKiroProviderSnapshot(
  settings: KiroSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: modelsFromSettings(settings),
      probe: settings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking Kiro CLI availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Kiro is disabled in T3 Code settings.",
          },
    });
  });
}

const runVersion = (settings: KiroSettings, environment: NodeJS.ProcessEnv) =>
  runKiroCommand(settings, ["--version"], environment);

const runKiroCommand = (
  settings: KiroSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = settings.binaryPath || "kiro-cli";
    const spawn = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawn.command, spawn.args, { env: environment, shell: spawn.shell }),
    );
  });

export const modelsFromKiroModelListResponse = (
  rawJson: string,
): Effect.Effect<ReadonlyArray<ServerProviderModel>, Schema.SchemaError> =>
  decodeKiroModelListResponse(rawJson).pipe(
    Effect.map((response) => {
      const seen = new Set<string>();
      const discovered = response.models.flatMap((model): Array<ServerProviderModel> => {
        const slug = model.model_id.trim();
        if (!slug || seen.has(slug)) return [];
        seen.add(slug);
        return [
          {
            slug,
            name: model.model_name.trim() || slug,
            isCustom: false,
            capabilities: EMPTY_CAPABILITIES,
          },
        ];
      });
      return discovered.length > 0 ? discovered : KIRO_BUILT_IN_MODELS;
    }),
  );

const discoverKiroModels = (settings: KiroSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const result = yield* runKiroCommand(
      settings,
      ["chat", "--list-models", "--format", "json"],
      environment,
    ).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.result);
    if (
      Result.isFailure(result) ||
      Option.isNone(result.success) ||
      result.success.value.code !== 0
    ) {
      return KIRO_BUILT_IN_MODELS;
    }
    return yield* modelsFromKiroModelListResponse(result.success.value.stdout).pipe(
      Effect.orElseSucceed(() => KIRO_BUILT_IN_MODELS),
    );
  });

export const checkKiroProviderStatus = Effect.fn("checkKiroProviderStatus")(function* (
  settings: KiroSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = modelsFromSettings(settings);
  if (!settings.enabled) {
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Kiro is disabled in T3 Code settings.",
      },
    });
  }
  const result = yield* runVersion(settings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(result)) {
    const error = result.failure;
    yield* Effect.logWarning("Kiro CLI health check failed.", { errorTag: error._tag });
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Kiro CLI (`kiro-cli`) is not installed or not on PATH."
          : "Failed to execute Kiro CLI health check.",
      },
    });
  }
  if (Option.isNone(result.success)) {
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Kiro CLI timed out while running `kiro-cli --version`.",
      },
    });
  }
  const output = result.success.value;
  const version = parseGenericCliVersion(`${output.stdout}\n${output.stderr}`);
  const models = yield* discoverKiroModels(settings, environment).pipe(
    Effect.map((discovered) =>
      providerModelsFromSettings(discovered, settings.customModels, EMPTY_CAPABILITIES),
    ),
  );
  return buildServerProvider({
    presentation: KIRO_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe:
      output.code === 0
        ? { installed: true, version, status: "ready", auth: { status: "unknown" } }
        : {
            installed: true,
            version,
            status: "error",
            auth: { status: "unknown" },
            message: "Kiro CLI is installed but failed to run.",
          },
  });
});

export const enrichKiroSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> =>
  enrichProviderSnapshotWithVersionAdvisory(input.snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap(input.publishSnapshot),
    Effect.catchCause((cause) =>
      Effect.logWarning("Kiro version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
