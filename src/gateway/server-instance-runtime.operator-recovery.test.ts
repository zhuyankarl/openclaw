import { expectDefined } from "@openclaw/normalization-core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { awaitGateBeforeSettlement, createDeferred } from "../../test/helpers/promise.js";
import { createRestartRecoveryRequesterSource } from "../agents/requester-run-recovery-source.js";
import { replaceSessionEntry } from "../config/sessions/session-accessor.js";
import {
  getAgentEventLifecycleGeneration,
  resetAgentEventsForTest,
  rotateAgentEventLifecycleGeneration,
} from "../infra/agent-events.js";
import { trackAsyncWork } from "../shared/async-work-scope.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import type { createAgentTurnService } from "./agent-turn/agent-turn-service.js";
import type { AgentTurnPrincipal } from "./agent-turn/types.js";
import { registerChatAbortController } from "./chat-abort.js";
import { createGatewayMethodRegistry } from "./methods/registry.js";
import { createOperatorRecoveryFixture } from "./operator-run-recovery.test-support.js";
import { createDirectChatContext } from "./server-chat.agent-events.test-helpers.js";
import { createGatewayInstanceRuntime } from "./server-instance-runtime.js";
import { authorizeGatewayMethod } from "./server-methods/method-authorization.js";

const startTurn = vi.hoisted(() => vi.fn<ReturnType<typeof createAgentTurnService>["startTurn"]>());

// Keep the typed runtime and principal capture real; no model/provider execution is needed
// to observe its authorization boundary and exact controller-registration lifetime.
// mock-isolation: Keep request-envelope plumbing isolated; the real method registry authorization is exercised below.
vi.mock("./server-methods.js", () => ({
  createRequestGatewayMethodRegistry: () => ({ isControlPlaneWrite: () => false }),
  runWithGatewayRequestEnvelope: async (
    _method: string,
    _client: unknown,
    run: () => Promise<unknown>,
  ) => await run(),
}));
// mock-isolation: Bypass unrelated ingress admission; this test exercises real restored-principal method authorization.
vi.mock("./server-methods/request-authorization.js", () => ({
  authorizeGatewayRequestPreDispatch: async () => ({ error: null }),
}));
// mock-isolation: Exclude model preparation while testing the typed recovery admission and exact owner lifetime.
vi.mock("./agent-turn/agent-request-preflight.js", () => ({
  prepareAgentRequestPreflight: ({ request }: { request: unknown }) => ({ request }),
}));
// mock-isolation: Control exact registration and await boundaries without starting provider execution.
vi.mock("./agent-turn/agent-turn-service.js", () => ({
  createAgentTurnService: () => ({ startTurn, waitForTurn: vi.fn() }),
}));

afterEach(() => {
  startTurn.mockReset();
  resetAgentEventsForTest();
});

describe("typed restart recovery operator admission", () => {
  it.each(["agentId", "sessionKey", "expectedExistingSessionId", "idempotencyKey"] as const)(
    "rejects dispatch before authority restoration when %s differs from the original claim",
    async (field) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const context = createDirectChatContext({ trackExecution: trackAsyncWork });
        const profile = ensureProfileForEmail("target-runtime@example.test");
        const fixture = await createOperatorRecoveryFixture({
          stateDir: state.stateDir,
          context,
          profileId: profile.id,
          config: {},
        });
        const runtime = createGatewayInstanceRuntime({
          getContext: () => context,
          getMethodRegistry: () => createGatewayMethodRegistry([]),
          isDispatchAvailable: () => true,
        });
        const request = {
          message: "Continue the interrupted turn",
          agentId: fixture.target.agentId,
          sessionKey: fixture.target.sessionKey,
          expectedExistingSessionId: fixture.target.sessionId,
          idempotencyKey: fixture.target.recoveryRunId,
        };
        request[field] = "another-claim";
        try {
          await expect(
            runtime.recovery.dispatchAgent(request, undefined, {
              restartRecoveryOperatorTarget: fixture.target,
            }),
          ).rejects.toThrow("dispatch does not match its source claim");
          expect(startTurn).not.toHaveBeenCalled();
        } finally {
          runtime.close();
        }
      });
    },
  );

  it.each([
    { source: "authenticated operator", operator: true, requester: "absent", expected: "accepted" },
    { source: "legacy claim", operator: false, requester: "absent", expected: "scope refusal" },
    { source: "matching requester", operator: true, requester: "matching", expected: "accepted" },
    {
      source: "native queued requester",
      operator: true,
      requester: "matching",
      expected: "accepted",
    },
    {
      source: "native queued mismatched requester",
      operator: true,
      requester: "different",
      expected: "identity refusal",
    },
    {
      source: "mismatched requester",
      operator: true,
      requester: "different",
      expected: "identity refusal",
    },
    {
      source: "requester without operator authority",
      operator: false,
      requester: "matching",
      expected: "identity refusal",
    },
  ] as const)(
    "applies recovery admission and the actual admin-method fence to $source",
    async ({ source, operator, requester, expected }) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const context = createDirectChatContext({ trackExecution: trackAsyncWork });
        const profile = ensureProfileForEmail("runtime-recovery@example.test");
        const fixture = await createOperatorRecoveryFixture({
          stateDir: state.stateDir,
          context,
          profileId: profile.id,
          config: {},
        });
        if (requester !== "absent") {
          await replaceSessionEntry(fixture.target, {
            ...fixture.entry,
            restartRecoveryRequesterSource: expectDefined(
              createRestartRecoveryRequesterSource({
                ...fixture.target,
                entry: fixture.entry,
                requesterOrigin: {
                  sessionKey: fixture.target.sessionKey,
                  operatorProfileId: requester === "matching" ? profile.id : "another-requester",
                },
              }),
              "host-captured requester source",
            ),
          });
        }
        const registry = createGatewayMethodRegistry([
          {
            name: "cron.add",
            scope: "operator.admin",
            owner: { kind: "core", area: "cron" },
            handler: vi.fn(),
          },
        ]);
        const runtime = createGatewayInstanceRuntime({
          getContext: () => context,
          getMethodRegistry: () => registry,
          isDispatchAvailable: () => true,
        });
        startTurn.mockImplementation(async ({ principal, io }) => {
          if (requester === "matching") {
            expect(principal?.internal?.agentToolCaller?.requesterOrigin).toMatchObject({
              sessionKey: fixture.target.sessionKey,
              operatorProfileId: profile.id,
            });
          }
          const authorization = authorizeGatewayMethod(
            "cron.add",
            principal,
            {},
            registry,
            context,
          );
          io.emitAcceptance(
            authorization.error
              ? [false, undefined, authorization.error]
              : [true, { runId: fixture.target.recoveryRunId, status: "ok" }, undefined],
          );
        });
        const request = {
          message: "Continue the interrupted turn",
          agentId: "main",
          sessionKey: fixture.target.sessionKey,
          expectedExistingSessionId: fixture.target.sessionId,
          idempotencyKey: fixture.target.recoveryRunId,
        };
        const queued = source.startsWith("native queued");
        const retained = queued
          ? expectDefined(await fixture.restore(), "queued source authority")
          : undefined;
        const options = queued
          ? {
              operatorRunAuthority: retained?.authority,
              agentToolCaller: {
                agentId: "main",
                sessionKey: fixture.target.sessionKey,
                requesterOrigin: {
                  sessionKey: fixture.target.sessionKey,
                  operatorProfileId: requester === "matching" ? profile.id : "another-requester",
                },
              },
            }
          : {
              restartRecoveryOperatorTarget: operator ? fixture.target : undefined,
              restartRecoveryRequesterTarget: requester !== "absent" ? fixture.target : undefined,
            };
        try {
          const result = runtime.recovery.dispatchAgent(request, undefined, options);
          if (expected === "accepted") {
            await expect(result).resolves.toMatchObject({ status: "ok" });
            expect(startTurn).toHaveBeenCalledOnce();
          } else if (expected === "identity refusal") {
            await expect(result).rejects.toThrow("requester has no matching operator authority");
            expect(startTurn).not.toHaveBeenCalled();
          } else {
            await expect(result).rejects.toThrow("missing scope: operator.admin");
            expect(startTurn).toHaveBeenCalledOnce();
          }
        } finally {
          retained?.release();
          runtime.close();
        }
      });
    },
  );

  it.each([
    "registration replaced",
    "registration aborted",
    "lifecycle rotated",
    "Gateway closed",
    "Gateway context replaced",
  ] as const)("refuses retained operator work after its exact %s", async (boundary) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const context = createDirectChatContext({ trackExecution: trackAsyncWork });
      let activeContext = context;
      const profile = ensureProfileForEmail("retained-runtime@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context,
        profileId: profile.id,
        config: {},
      });
      const registration = registerChatAbortController({
        chatAbortControllers: context.chatAbortControllers,
        runId: fixture.target.recoveryRunId,
        agentId: "main",
        sessionKey: fixture.target.sessionKey,
        sessionId: fixture.target.sessionId,
        lifecycleGeneration: getAgentEventLifecycleGeneration(),
        timeoutMs: 60_000,
        kind: "agent",
      });
      if (!registration.registered) {
        throw new Error("Expected a fresh controller registration");
      }
      const runtime = createGatewayInstanceRuntime({
        getContext: () => activeContext,
        getMethodRegistry: () => createGatewayMethodRegistry([]),
        isDispatchAvailable: () => true,
      });
      const entered = createDeferred<AgentTurnPrincipal>();
      const finish = createDeferred();
      let execution: Promise<void> | undefined;
      startTurn.mockImplementation(({ principal, io }) => {
        execution = (async () => {
          const currentPrincipal = expectDefined(principal, "typed recovery principal");
          io.emitStartOwner?.(fixture.target.recoveryRunId, registration.entry);
          io.emitAcceptance(
            [true, { runId: fixture.target.recoveryRunId, status: "accepted" }, undefined],
            {
              runId: fixture.target.recoveryRunId,
            },
          );
          entered.resolve(currentPrincipal);
          await finish.promise;
          io.emitFinal([true, { runId: fixture.target.recoveryRunId, status: "ok" }, undefined]);
        })();
        return execution;
      });
      const options = { restartRecoveryOperatorTarget: fixture.target, expectFinal: true };
      const pending = runtime.recovery.dispatchAgent(
        {
          message: "Continue the interrupted turn",
          agentId: "main",
          sessionKey: fixture.target.sessionKey,
          expectedExistingSessionId: fixture.target.sessionId,
          idempotencyKey: fixture.target.recoveryRunId,
        },
        undefined,
        options,
      );
      try {
        const principal = await awaitGateBeforeSettlement(
          entered.promise,
          pending,
          "Recovery dispatch settled before publishing its typed principal",
        );
        const authority = expectDefined(
          principal.internal?.operatorRunAuthority,
          "restored typed authority",
        );
        expect(authority.assertCurrent).not.toThrow();
        if (boundary === "registration replaced") {
          // Same correlation identity cannot transfer the old registration's authority.
          context.chatAbortControllers.set(fixture.target.recoveryRunId, {
            ...registration.entry,
            controller: new AbortController(),
          });
        } else if (boundary === "registration aborted") {
          registration.controller.abort(new Error("Recovery aborted"));
        } else if (boundary === "lifecycle rotated") {
          rotateAgentEventLifecycleGeneration();
        } else if (boundary === "Gateway closed") {
          runtime.close();
        } else {
          activeContext = createDirectChatContext({ trackExecution: trackAsyncWork });
        }
        expect(authority.assertCurrent).toThrow();
        if (boundary === "registration replaced") {
          // Once refusal is observed, putting the same old row back cannot revive tools.
          context.chatAbortControllers.set(fixture.target.recoveryRunId, registration.entry);
          expect(authority.assertCurrent).toThrow();
        }
      } finally {
        finish.resolve();
        await pending.catch(() => undefined);
        await execution;
        registration.cleanup();
        runtime.close();
      }
    });
  });
});
