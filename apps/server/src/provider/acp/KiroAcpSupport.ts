/**
 * KiroAcpSupport — spawn wiring for Kiro CLI's native ACP server.
 *
 * Kiro resolves credentials before the ACP process starts and advertises no
 * ACP authentication methods, so this intentionally omits `authMethodId`.
 *
 * @module provider/acp/KiroAcpSupport
 */
import { type KiroSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";
import { normalizeModelSlug } from "@t3tools/shared/model";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const KIRO_DRIVER_KIND = ProviderDriverKind.make("kiro");
export const KIRO_AUTO_MODEL = "auto";

type KiroAcpRuntimeSettings = Pick<KiroSettings, "agent" | "binaryPath">;

interface KiroAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly kiroSettings: KiroAcpRuntimeSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
  readonly model?: string | null | undefined;
}

export function resolveKiroAcpBaseModelId(model: string | null | undefined): string {
  const trimmed = model?.trim();
  const base = trimmed && trimmed.length > 0 ? trimmed : KIRO_AUTO_MODEL;
  return normalizeModelSlug(base, KIRO_DRIVER_KIND) ?? KIRO_AUTO_MODEL;
}

export function buildKiroAcpSpawnInput(
  kiroSettings: KiroAcpRuntimeSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  model?: string | null | undefined,
): AcpSessionRuntime.AcpSpawnInput {
  const agent = kiroSettings?.agent?.trim();
  const resolvedModel = resolveKiroAcpBaseModelId(model);
  return {
    command: kiroSettings?.binaryPath || "kiro-cli",
    args: [
      "acp",
      ...(agent ? (["--agent", agent] as const) : []),
      ...(resolvedModel === KIRO_AUTO_MODEL ? [] : (["--model", resolvedModel] as const)),
    ],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeKiroAcpRuntime = (
  input: KiroAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildKiroAcpSpawnInput(
          input.kiroSettings,
          input.cwd,
          input.environment,
          input.model,
        ),
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });
