/**
 * GithubCopilotAcpSupport — spawn and model plumbing for the GitHub Copilot
 * CLI's native ACP server (`copilot --acp`).
 *
 * Copilot differs from the other ACP providers in one way that shapes this
 * module: `session/new` does not advertise a `SessionModelState`, so there is
 * no `session/set_model` to call. The model is chosen once, on the command
 * line, which is why `buildGithubCopilotAcpSpawnInput` takes the model and the
 * adapter reports `sessionModelSwitch: "unsupported"`.
 *
 * @module provider/acp/GithubCopilotAcpSupport
 */
import { type GithubCopilotSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";
import { normalizeModelSlug } from "@t3tools/shared/model";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

/**
 * The single auth method `copilot --acp` advertises. Copilot resolves
 * credentials from `~/.copilot`, `GH_TOKEN`, or `GITHUB_TOKEN`; when it
 * already has them, `authenticate` is a no-op that returns `{}`.
 */
const GITHUB_COPILOT_AUTH_METHOD = "copilot-login";
const GITHUB_COPILOT_DRIVER_KIND = ProviderDriverKind.make("githubCopilot");

/** Copilot's own "let the CLI choose" sentinel, and our default model slug. */
export const GITHUB_COPILOT_AUTO_MODEL = "auto";

type GithubCopilotAcpRuntimeSettings = Pick<GithubCopilotSettings, "binaryPath">;

interface GithubCopilotAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "clientCapabilities" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly githubCopilotSettings: GithubCopilotAcpRuntimeSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
  /**
   * Model slug passed as `--model`. Omitted (or `"auto"`) leaves the choice to
   * Copilot, which is what the CLI does by default.
   */
  readonly model?: string | null | undefined;
}

export function buildGithubCopilotAcpSpawnInput(
  githubCopilotSettings: GithubCopilotAcpRuntimeSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  model?: string | null | undefined,
): AcpSessionRuntime.AcpSpawnInput {
  const resolvedModel = resolveGithubCopilotAcpBaseModelId(model);
  return {
    command: githubCopilotSettings?.binaryPath || "copilot",
    args: [
      "--acp",
      ...(resolvedModel === GITHUB_COPILOT_AUTO_MODEL ? [] : (["--model", resolvedModel] as const)),
    ],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeGithubCopilotAcpRuntime = (
  input: GithubCopilotAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildGithubCopilotAcpSpawnInput(
          input.githubCopilotSettings,
          input.cwd,
          input.environment,
          input.model,
        ),
        authMethodId: GITHUB_COPILOT_AUTH_METHOD,
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

export function resolveGithubCopilotAcpBaseModelId(model: string | null | undefined): string {
  const trimmed = model?.trim();
  const base = trimmed && trimmed.length > 0 ? trimmed : GITHUB_COPILOT_AUTO_MODEL;
  return normalizeModelSlug(base, GITHUB_COPILOT_DRIVER_KIND) ?? GITHUB_COPILOT_AUTO_MODEL;
}
