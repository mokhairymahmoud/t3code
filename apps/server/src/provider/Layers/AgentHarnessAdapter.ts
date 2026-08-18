/**
 * AgentHarnessAdapter — ACP adapter for the Agent Harness binary.
 *
 * Agent Harness exposes ACP over stdio via `agent-harness acp`. The adapter
 * spawns the binary, initializes a session, and routes prompt/event traffic
 * through the shared AcpSessionRuntime infrastructure.
 *
 * @module provider/Layers/AgentHarnessAdapter
 */
import {
  ApprovalRequestId,
  type AgentHarnessSettings,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { ProviderAdapterError } from "../Errors.ts";

const PROVIDER = ProviderDriverKind.make("agentHarness");
const AGENT_HARNESS_RESUME_VERSION = 1 as const;

export interface AgentHarnessAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface AgentHarnessAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface AgentHarnessSessionContext {
  readonly threadId: ThreadId;
  readonly acpSessionId: string;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly scope: Scope.Closeable;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  session: ProviderSession;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  stopped: boolean;
}

function parseResumeCursor(raw: unknown): { sessionId: string } | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const value = raw as Record<string, unknown>;
  return value.schemaVersion === AGENT_HARNESS_RESUME_VERSION &&
    typeof value.sessionId === "string" &&
    value.sessionId.trim()
    ? { sessionId: value.sessionId.trim() }
    : undefined;
}

function permissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  return request.options.find((option) => option.kind === kind)?.optionId.trim() || undefined;
}

function buildAgentHarnessAcpSpawnInput(
  settings: AgentHarnessSettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  model?: string,
): AcpSessionRuntime.AcpSpawnInput {
  // AgentHarness reads its model only at process startup. T3 Code's picker
  // supplies the selected deployment on the session, so carry it into this
  // child process rather than silently falling back to the parent's .env.
  const env = model ? { ...environment, AZURE_OPENAI_MODEL: model } : environment;
  return {
    command: settings.binaryPath || "agent-harness",
    args: ["acp"],
    cwd,
    ...(env ? { env } : {}),
  };
}

export const makeAgentHarnessAdapter = Effect.fn("makeAgentHarnessAdapter")(function* (
  settings: AgentHarnessSettings,
  options?: AgentHarnessAdapterLiveOptions,
): Effect.fn.Return<
  AgentHarnessAdapterShape,
  never,
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ChildProcessSpawner.ChildProcessSpawner
  | ServerConfig
  | Scope.Scope
> {
  const instanceId = options?.instanceId ?? ProviderInstanceId.make("agentHarness");
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* ServerConfig;
  const sessions = new Map<ThreadId, AgentHarnessSessionContext>();
  const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomId = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate an Agent Harness runtime identifier.",
          cause,
        }),
    ),
  );
  const stamp = () =>
    Effect.all({ eventId: Effect.map(randomId, EventId.make), createdAt: nowIso });
  const publish = (event: ProviderRuntimeEvent) =>
    PubSub.publish(events, event).pipe(Effect.asVoid);
  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<AgentHarnessSessionContext, ProviderAdapterSessionNotFoundError> => {
    const context = sessions.get(threadId);
    return context && !context.stopped
      ? Effect.succeed(context)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };
  const closeSession = (context: AgentHarnessSessionContext) =>
    Effect.gen(function* () {
      if (context.stopped) return;
      context.stopped = true;
      yield* Effect.forEach(
        context.pendingApprovals.values(),
        (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
        { discard: true },
      );
      if (context.notificationFiber) yield* Fiber.interrupt(context.notificationFiber);
      yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
      sessions.delete(context.threadId);
      yield* publish({
        type: "session.exited",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: context.threadId,
        payload: { exitKind: "graceful" },
      });
    });

  const startSession: AgentHarnessAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
        });
      }
      if (!input.cwd?.trim()) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "cwd is required and must be non-empty.",
        });
      }
      const existing = sessions.get(input.threadId);
      if (existing) yield* closeSession(existing);
      const cwd = path.resolve(input.cwd);
      const scope = yield* Scope.make("sequential");
      let transferred = false;
      yield* Effect.addFinalizer(() => (transferred ? Effect.void : Scope.close(scope, Exit.void)));
      const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
      const resumeSessionId = parseResumeCursor(input.resumeCursor)?.sessionId;
      const mcp = McpProviderSession.readMcpProviderSession(input.threadId);
      const acp = yield* Effect.gen(function* () {
        const acpContext = yield* Layer.build(
          AcpSessionRuntime.layer({
            spawn: buildAgentHarnessAcpSpawnInput(
              settings,
              cwd,
              options?.environment,
              input.modelSelection?.model,
            ),
            cwd,
            clientInfo: { name: "t3code", version: "1.0.0" },
            ...(resumeSessionId ? { resumeSessionId } : {}),
            ...(mcp
              ? {
                  mcpServers: [
                    {
                      type: "http" as const,
                      name: "t3-code",
                      url: mcp.endpoint,
                      headers: [{ name: "Authorization", value: mcp.authorizationHeader }],
                    },
                  ],
                }
              : {}),
          }).pipe(Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner))),
        );
        return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
          Effect.provide(acpContext),
        );
      }).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(Scope.Scope, scope),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail:
                typeof cause === "object" && cause !== null && "message" in cause
                  ? (cause as { message: string }).message
                  : String(cause),
              cause,
            }),
        ),
      );
      yield* acp.handleRequestPermission((request) =>
        Effect.gen(function* () {
          const autoOption =
            input.runtimeMode === "full-access"
              ? (permissionOptionId(request, "acceptForSession") ??
                permissionOptionId(request, "accept"))
              : undefined;
          if (autoOption)
            return { outcome: { outcome: "selected" as const, optionId: autoOption } };
          const requestId = ApprovalRequestId.make(yield* randomId);
          const decision = yield* Deferred.make<ProviderApprovalDecision>();
          const context = sessions.get(input.threadId);
          pendingApprovals.set(requestId, { decision });
          const permission = parsePermissionRequest(request);
          yield* publish(
            makeAcpRequestOpenedEvent({
              stamp: yield* stamp(),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId: context?.activeTurnId,
              requestId: RuntimeRequestId.make(requestId),
              permissionRequest: permission,
              detail: permission.detail ?? "Agent Harness requests permission.",
              args: request,
              source: "acp.jsonrpc",
              method: "session/request_permission",
              rawPayload: request,
            }),
          );
          const resolved = yield* Deferred.await(decision);
          pendingApprovals.delete(requestId);
          yield* publish(
            makeAcpRequestResolvedEvent({
              stamp: yield* stamp(),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId: context?.activeTurnId,
              requestId: RuntimeRequestId.make(requestId),
              permissionRequest: permission,
              decision: resolved,
            }),
          );
          const optionId =
            resolved === "cancel" ? undefined : permissionOptionId(request, resolved);
          return optionId
            ? { outcome: { outcome: "selected" as const, optionId } }
            : { outcome: { outcome: "cancelled" as const } };
        }).pipe(
          Effect.mapError(
            (cause) =>
              new EffectAcpErrors.AcpTransportError({
                detail: "Failed to process Agent Harness ACP permission request.",
                cause,
              }),
          ),
        ),
      );
      const started = yield* acp
        .start()
        .pipe(
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", cause),
          ),
        );
      const createdAt = yield* nowIso;
      const context: AgentHarnessSessionContext = {
        threadId: input.threadId,
        acpSessionId: started.sessionId,
        acp,
        scope,
        pendingApprovals,
        notificationFiber: undefined,
        activeTurnId: undefined,
        turns: [],
        stopped: false,
        session: {
          provider: PROVIDER,
          providerInstanceId: instanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          model: input.modelSelection?.model ?? "default",
          threadId: input.threadId,
          resumeCursor: {
            schemaVersion: AGENT_HARNESS_RESUME_VERSION,
            sessionId: started.sessionId,
          },
          createdAt,
          updatedAt: createdAt,
        },
      };
      sessions.set(input.threadId, context);
      context.notificationFiber = yield* Stream.runForEach(acp.getEvents(), (event) =>
        Effect.gen(function* () {
          if (event._tag === "EventStreamBarrier") {
            yield* Deferred.succeed(event.acknowledge, undefined);
            return;
          }
          const turnId = context.activeTurnId;
          const eventStamp = yield* stamp();
          switch (event._tag) {
            case "AssistantItemStarted":
            case "AssistantItemCompleted":
              yield* publish(
                makeAcpAssistantItemEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  itemId: event.itemId,
                  lifecycle:
                    event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
                }),
              );
              return;
            case "ContentDelta":
              yield* publish(
                makeAcpContentDeltaEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  ...(event.itemId ? { itemId: event.itemId } : {}),
                  text: event.text,
                  rawPayload: event.rawPayload,
                }),
              );
              return;
            case "ToolCallUpdated":
              yield* publish(
                makeAcpToolCallEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  toolCall: event.toolCall,
                  rawPayload: event.rawPayload,
                }),
              );
              return;
            case "PlanUpdated":
              yield* publish(
                makeAcpPlanUpdatedEvent({
                  stamp: eventStamp,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  payload: event.payload,
                  source: "acp.jsonrpc",
                  method: "session/update",
                  rawPayload: event.rawPayload,
                }),
              );
              return;
            case "ModeChanged":
              return;
          }
        }),
      ).pipe(
        Effect.catch(() => Effect.void),
        Effect.forkIn(scope),
      );
      transferred = true;
      yield* publish({
        type: "session.started",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        payload: { resume: started.initializeResult },
      });
      yield* publish({
        type: "session.state.changed",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        payload: { state: "ready", reason: "Agent Harness ACP session ready" },
      });
      yield* publish({
        type: "thread.started",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        payload: { providerThreadId: started.sessionId },
      });
      return context.session;
    }).pipe(Effect.scoped);

  const sendTurn: AgentHarnessAdapterShape["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      if (context.activeTurnId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Agent Harness is already processing a turn.",
        });
      }
      const text = input.input?.trim();
      const images = yield* Effect.forEach(input.attachments ?? [], (attachment) =>
        Effect.gen(function* () {
          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!attachmentPath)
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/prompt",
              detail: `Invalid attachment id '${attachment.id}'.`,
            });
          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          return {
            type: "image" as const,
            data: Buffer.from(bytes).toString("base64"),
            mimeType: attachment.mimeType,
          };
        }),
      );
      const prompt: Array<EffectAcpSchema.ContentBlock> = [
        ...(text ? [{ type: "text" as const, text }] : []),
        ...images,
      ];
      if (prompt.length === 0)
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Turn requires non-empty text or attachments.",
        });
      const turnId = TurnId.make(yield* randomId);
      context.activeTurnId = turnId;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        updatedAt: yield* nowIso,
      };
      yield* publish({
        type: "turn.started",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        payload: { model: context.session.model },
      });
      const result = yield* context.acp
        .prompt({ prompt })
        .pipe(
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", cause),
          ),
        );
      yield* context.acp.drainEvents;
      context.turns.push({ id: turnId, items: [{ prompt, result }] });
      context.activeTurnId = undefined;
      const { activeTurnId: _activeTurnId, ...ready } = context.session;
      context.session = { ...ready, status: "ready", updatedAt: yield* nowIso };
      yield* publish({
        type: "turn.completed",
        ...(yield* stamp()),
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        payload: {
          state: result.stopReason === "cancelled" ? "cancelled" : "completed",
          stopReason: result.stopReason,
        },
      });
      return { threadId: input.threadId, turnId, resumeCursor: context.session.resumeCursor };
    });

  const interruptTurn: AgentHarnessAdapterShape["interruptTurn"] = (threadId, turnId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      if (turnId !== undefined && context.activeTurnId !== turnId) return;
      yield* context.acp.cancel.pipe(Effect.ignore);
    });
  const respondToRequest: AgentHarnessAdapterShape["respondToRequest"] = (
    threadId,
    requestId,
    decision,
  ) =>
    Effect.gen(function* () {
      const pending = (yield* requireSession(threadId)).pendingApprovals.get(requestId);
      if (!pending)
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail: `Unknown pending approval request: ${requestId}`,
        });
      yield* Deferred.succeed(pending.decision, decision);
    });
  const respondToUserInput: AgentHarnessAdapterShape["respondToUserInput"] = (
    threadId,
    requestId,
  ) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "session/user_input",
        detail: `Agent Harness does not expose ACP user-input requests: ${requestId}`,
      });
    });
  const stopSession: AgentHarnessAdapterShape["stopSession"] = (threadId) =>
    requireSession(threadId).pipe(Effect.flatMap(closeSession));
  const listSessions: AgentHarnessAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), (context) => context.session));
  const hasSession: AgentHarnessAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => sessions.has(threadId));
  const readThread: AgentHarnessAdapterShape["readThread"] = (threadId) =>
    requireSession(threadId).pipe(Effect.map((context) => ({ threadId, turns: context.turns })));
  const rollbackThread: AgentHarnessAdapterShape["rollbackThread"] = (threadId, numTurns) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "thread/rollback",
        detail: `Agent Harness ACP sessions do not support provider-side rollback (${numTurns} turns requested).`,
      });
    });
  const stopAll: AgentHarnessAdapterShape["stopAll"] = () =>
    Effect.forEach(Array.from(sessions.values()), closeSession, { discard: true });
  yield* Effect.addFinalizer(() =>
    stopAll().pipe(Effect.ignore, Effect.andThen(PubSub.shutdown(events))),
  );
  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents: Stream.fromPubSub(events),
  } satisfies AgentHarnessAdapterShape;
});
