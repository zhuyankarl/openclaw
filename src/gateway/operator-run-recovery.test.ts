import { expectDefined } from "@openclaw/normalization-core";
import { describe, expect, it, vi } from "vitest";
import { createRestartRecoveryRequesterSource } from "../agents/requester-run-recovery-source.js";
import { getCommandOwnerAuthority } from "../auto-reply/command-owner-authority.js";
import { withAdminIngress } from "../channels/message-access/operator-authority.test-support.js";
import { setRuntimeConfigSnapshot } from "../config/runtime-snapshot.js";
import {
  persistSessionTranscriptTurn,
  replaceSessionEntry,
} from "../config/sessions/session-accessor.js";
import { buildRestartRecoveryExpectedState } from "../config/sessions/session-transcript-turn-state.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { issueDeviceBootstrapToken } from "../infra/device-bootstrap.js";
import { persistDevicePairingStoreState } from "../infra/device-pairing-store.js";
import { ensureDeviceToken, rotateDeviceToken } from "../infra/device-pairing-tokens.js";
import { getPairedDevice, updatePairedDeviceMetadata } from "../infra/device-pairing.js";
import type { PairedDevice } from "../infra/device-pairing.types.js";
import type { PluginGatewayAccessPolicy } from "../plugins/gateway-access-policy.types.js";
import { createEmptyPluginRegistry } from "../plugins/registry-empty.js";
import {
  captureActivePluginRegistrySnapshot,
  restoreActivePluginRegistrySnapshot,
  stageActivePluginRegistry,
} from "../plugins/runtime.js";
import { createPluginRecord } from "../plugins/status.test-helpers.js";
import { changeCanonicalUserChannelIdentity } from "../state/user-channel-identity-operations.js";
import { linkEmail, setUserProfileRole } from "../state/user-profile-writes.worker.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import { withOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { captureAgentTurnPrincipal } from "./agent-turn/principal.js";
import {
  captureGatewayDeviceRevocation,
  invalidateGatewayDeviceRevocation,
} from "./device-revocation.js";
import { retireDeviceTokenClients } from "./device-token-client-lifecycle.js";
import { captureGatewayOperatorRunAuthority } from "./operator-run-authority.js";
import { decodeQueuedOperatorRecoverySource } from "./operator-run-recovery-source.codec.js";
import { captureQueuedOperatorRecoverySource } from "./operator-run-recovery-source.js";
import {
  restoreGatewayRunRecovery,
  restoreGatewayQueuedOperatorRecovery,
} from "./operator-run-recovery.js";
import { createOperatorRecoveryFixture } from "./operator-run-recovery.test-support.js";
import { createContext } from "./server-plugin-in-process-dispatch.test-support.js";
import {
  enforceSharedGatewaySessionGenerationForConfigWrite,
  SharedGatewaySessionGenerationState,
} from "./server-shared-auth-generation.js";

function pairedOperator(): PairedDevice {
  return {
    deviceId: "recovery-device",
    publicKey: "fixture-public-key",
    roles: ["operator"],
    approvedScopes: ["operator.admin"],
    tokens: {
      operator: {
        token: "fixture-device-token",
        role: "operator",
        scopes: ["operator.admin"],
        createdAtMs: 1,
      },
    },
    createdAtMs: 1,
    approvedAtMs: 1,
  };
}

describe("restart recovery authenticated operator source", () => {
  it("resumes a queued channel source after ingress completion but rejects an unlink and relink", async () => {
    await withAdminIngress(
      async ({ admins, context, gateway, retire }) => {
        const admin = expectDefined(admins[0], "linked administrator");
        const original = expectDefined(
          getCommandOwnerAuthority(await context(admin.identity.senderId))?.operatorAuthority,
          "authenticated channel operator",
        );
        const source = expectDefined(
          captureQueuedOperatorRecoverySource(original),
          "queued source",
        );
        expect(source.kind).toBe("channel");
        const serializedSource = JSON.stringify(source);
        const persisted: unknown = JSON.parse(serializedSource);
        retire();
        expect(original.assertCurrent).toThrow();
        const restore = () =>
          restoreGatewayQueuedOperatorRecovery({
            source: decodeQueuedOperatorRecoverySource(persisted),
            context: gateway,
            assertCurrent: () => {},
          });
        const restored = await restore();
        try {
          expect(restored.authority.assertCurrent).not.toThrow();
          expect(restored.authority.profileId).toBe(admin.profile.id);
          await changeCanonicalUserChannelIdentity("unlink", admin.profile.id, admin.identity);
          await changeCanonicalUserChannelIdentity("link", admin.profile.id, admin.identity);
          expect(restored.authority.assertCurrent).toThrow();
          await expect(restore()).rejects.toThrow("Queued channel operator authority changed");
        } finally {
          restored.release();
        }
      },
      "role",
      "slack",
    );
  });

  it("restores only the requester bound to the current durable source claim", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("requester-custody@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
      });
      let current = true;
      const restore = async () =>
        (
          await restoreGatewayRunRecovery({
            target: fixture.target,
            context: fixture.context,
            restoreOperator: false,
            restoreRequester: true,
            assertCurrent: () => {
              if (!current) {
                throw new Error("recovery claim retired");
              }
            },
          })
        ).requesterOrigin;
      await expect(restore()).resolves.toBeUndefined();
      const origin = {
        sessionKey: "agent:research:telegram:group:requesters",
        messageProvider: "telegram",
        senderId: "bob",
      };
      const source = expectDefined(
        createRestartRecoveryRequesterSource({
          ...fixture.target,
          entry: fixture.entry,
          requesterOrigin: origin,
        }),
        "host-captured requester source",
      );
      const bound = { ...fixture.entry, restartRecoveryRequesterSource: source };
      await replaceSessionEntry(fixture.target, bound);
      origin.senderId = "alice";
      origin.sessionKey = "agent:research:telegram:group:replacement";
      await expect(restore()).resolves.toMatchObject({
        senderId: "bob",
        sessionKey: "agent:research:telegram:group:requesters",
      });
      const historical = structuredClone(source);
      Object.assign(historical.requesterOrigin, {
        senderIsOwner: true,
        conversationToolPolicy: { allow: ["*"] },
      });
      await replaceSessionEntry(fixture.target, {
        ...bound,
        restartRecoveryRequesterSource: historical,
      });
      const restoredFacts = await restore();
      expect(restoredFacts).not.toHaveProperty("senderIsOwner");
      expect(restoredFacts).not.toHaveProperty("conversationToolPolicy");
      for (const replacement of [
        { ...source, sessionId: "copied-session" },
        { ...source, sessionKey: "agent:main:copied" },
        { ...source, sourceRunId: "copied-run" },
        { ...source, lifecycleRevision: "replacement-lifecycle" },
      ]) {
        await replaceSessionEntry(fixture.target, {
          ...bound,
          restartRecoveryRequesterSource: replacement,
        });
        await expect(restore()).resolves.toBeUndefined();
      }
      for (const malformed of [
        { ...source.requesterOrigin, sessionKey: "" },
        { ...source.requesterOrigin, senderId: 7 },
        { ...source.requesterOrigin, senderId: undefined },
        { ...source.requesterOrigin, messageThreadId: {} },
      ]) {
        const invalid = structuredClone(source);
        Reflect.set(invalid, "requesterOrigin", malformed);
        await replaceSessionEntry(fixture.target, {
          ...bound,
          restartRecoveryRequesterSource: invalid,
        });
        await expect(restore()).resolves.toBeUndefined();
      }
      await replaceSessionEntry(fixture.target, {
        ...bound,
        restartRecoveryDeliveryRunId: "replacement-run",
      });
      await expect(restore()).rejects.toThrow("no longer owns this input");
      await replaceSessionEntry(fixture.target, bound);
      const pending = restore();
      current = false;
      await expect(pending).rejects.toThrow("recovery claim retired");
    });
  });

  it("keeps restored authority current during unrelated issuance", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("publication-lifetime@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
        device: pairedOperator(),
      });
      const restored = expectDefined(await fixture.restore(), "exact device source");
      try {
        await issueDeviceBootstrapToken({ baseDir: state.stateDir });
        expect(restored.authority.signal?.aborted).toBe(false);
        expect(restored.authority.assertCurrent).not.toThrow();
      } finally {
        restored.release();
      }
    });
  });
  it("does not adopt an incidental paired token for tokenless trusted-proxy ingress", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("proxy-source@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
        device: pairedOperator(),
        tokenlessDevice: true,
      });
      expect(fixture.entry.restartRecoveryOperatorSource?.snapshot.device).toBeUndefined();
      const restored = expectDefined(await fixture.restore(), "proxy-owned source");
      try {
        expect(
          (await rotateDeviceToken({ deviceId: "recovery-device", role: "operator" })).ok,
        ).toBe(true);
        expect(restored.authority.signal?.aborted).toBe(false);
        expect(restored.authority.assertCurrent).not.toThrow();
      } finally {
        restored.release();
      }
    });
  });
  it.each(["before capture", "after admission", "after restoration"] as const)(
    "revokes the original device authority on Gateway retirement %s",
    async (timing) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const profile = ensureProfileForEmail("exact-token@example.test");
        const fixture = await createOperatorRecoveryFixture({
          stateDir: state.stateDir,
          context: createContext(),
          profileId: profile.id,
          config: {},
          device: pairedOperator(),
        });
        const caller = captureGatewayDeviceRevocation(
          fixture.context,
          { deviceId: "recovery-device", role: "operator" },
          () => !fixture.client.invalidated,
        );
        fixture.context.invalidateClientsForDevice = (deviceId, options) => {
          invalidateGatewayDeviceRevocation(fixture.context, deviceId, options?.role);
          fixture.client.invalidated = true;
        };
        const restored =
          timing === "after restoration"
            ? expectDefined(await fixture.restore(), "restored device source")
            : undefined;
        const accepted =
          timing === "after admission"
            ? expectDefined(
                await captureGatewayOperatorRunAuthority({
                  client: captureAgentTurnPrincipal(fixture.client),
                  context: fixture.context,
                  sourceAuthority: null,
                  hasCurrentClientAuthority: caller.isCurrent,
                }),
                "accepted exact original token",
              )
            : undefined;
        try {
          const rotated = await rotateDeviceToken({
            deviceId: "recovery-device",
            role: "operator",
          });
          expect(rotated.ok).toBe(true);
          retireDeviceTokenClients(
            fixture.context,
            "recovery-device",
            ["operator"],
            "device-token-rotated",
          );
          expect(fixture.client.invalidated).toBe(true);
          const source = restored ?? accepted;
          if (source) {
            expect(source.authority.signal?.aborted).toBe(true);
            expect(source.authority.assertCurrent).toThrow();
          } else {
            await expect(
              captureGatewayOperatorRunAuthority({
                client: captureAgentTurnPrincipal(fixture.client),
                context: fixture.context,
                sourceAuthority: null,
                hasCurrentClientAuthority: caller.isCurrent,
              }),
            ).rejects.toThrow("Gateway caller authority is no longer active");
          }
        } finally {
          restored?.release();
          accepted?.release();
          caller.release();
        }
      });
    },
  );
  it("keeps restored authority when a same-device handshake reissues its token", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("issuer-replacement@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
        device: pairedOperator(),
      });
      const restored = expectDefined(await fixture.restore(), "original device source");
      try {
        expect(
          await ensureDeviceToken({
            deviceId: "recovery-device",
            role: "operator",
            scopes: ["operator.admin"],
            issuer: { kind: "shared-gateway-auth", generation: "replacement-issuer" },
          }),
        ).not.toBeNull();
        expect(restored.authority.signal?.aborted).toBe(false);
        expect(restored.authority.assertCurrent).not.toThrow();
      } finally {
        restored.release();
      }
    });
  });
  it("keeps accepted device authority through unrelated pairing traffic until Gateway retirement", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("pairing-traffic@example.test");
      const device = pairedOperator();
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
        device,
      });
      const caller = captureGatewayDeviceRevocation(
        fixture.context,
        { deviceId: device.deviceId, role: "operator" },
        () => !fixture.client.invalidated,
      );
      fixture.context.invalidateClientsForDevice = (deviceId, options) => {
        invalidateGatewayDeviceRevocation(fixture.context, deviceId, options?.role);
        fixture.client.invalidated = true;
      };
      const accepted = expectDefined(
        await captureGatewayOperatorRunAuthority({
          client: captureAgentTurnPrincipal(fixture.client),
          context: fixture.context,
          sourceAuthority: null,
          hasCurrentClientAuthority: caller.isCurrent,
        }),
        "accepted paired operator",
      );
      try {
        // A sibling owner changes the revision before a partial lookup publishes it.
        const unrelated = { ...pairedOperator(), deviceId: "unrelated-device" };
        persistDevicePairingStoreState(
          {
            pendingById: {},
            pairedByDeviceId: { [device.deviceId]: device, [unrelated.deviceId]: unrelated },
          },
          state.stateDir,
          "paired",
        );
        expect((await getPairedDevice(unrelated.deviceId))?.deviceId).toBe(unrelated.deviceId);
        expect(accepted.authority.signal?.aborted).toBe(false);
        expect(accepted.authority.assertCurrent).not.toThrow();
        expect(
          await updatePairedDeviceMetadata(unrelated.deviceId, { displayName: "Renamed" }),
        ).toBe(true);
        expect(accepted.authority.signal?.aborted).toBe(false);
        expect(accepted.authority.assertCurrent).not.toThrow();
        retireDeviceTokenClients(
          fixture.context,
          device.deviceId,
          ["operator"],
          "device-token-revoked",
        );
        expect(fixture.client.invalidated).toBe(true);
        expect(accepted.authority.signal?.aborted).toBe(true);
        expect(accepted.authority.assertCurrent).toThrow();
      } finally {
        accepted.release();
        caller.release();
      }
    });
  });
  it.each(["before restoration", "after admission"] as const)(
    "refuses recovery after shared credentials rotate %s",
    async (timing) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const profile = ensureProfileForEmail("shared-generation@example.test");
        const context = createContext();
        const generation = "original-credential-generation";
        const owner = new SharedGatewaySessionGenerationState({
          current: generation,
          required: null,
        });
        context.sharedGatewaySessionGenerationState = owner;
        const fixture = await createOperatorRecoveryFixture({
          stateDir: state.stateDir,
          context,
          profileId: profile.id,
          config: {},
          sharedGeneration: generation,
        });
        const restored =
          timing === "after admission"
            ? expectDefined(await fixture.restore(), "accepted shared credential source")
            : undefined;
        try {
          enforceSharedGatewaySessionGenerationForConfigWrite({
            state: owner,
            nextConfig: {},
            resolveRuntimeSnapshotGeneration: () => "replacement-credential-generation",
            clients: [],
          });
          if (restored) {
            expect(restored.authority.signal?.aborted).toBe(true);
            expect(restored.authority.assertCurrent).toThrow("authority changed");
            owner.publish({ current: generation, required: null });
            expect(restored.authority.assertCurrent).toThrow("authority changed");
          } else {
            await expect(fixture.restore()).rejects.toThrow("authentication owner changed");
          }
        } finally {
          restored?.release();
        }
      });
    },
  );
  it("requires the original access-plugin grant before restoration and after admission", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const original = { pluginId: "recovery-access", grantId: "original-accepted-grant" };
      let permitted = true;
      const controller = new AbortController();
      const grant = {
        grantId: original.grantId,
        signal: controller.signal,
        assertCurrent: () => controller.signal.throwIfAborted(),
      };
      const resume = vi.fn<NonNullable<PluginGatewayAccessPolicy["resume"]>>(() =>
        permitted ? grant : undefined,
      );
      const registry = createEmptyPluginRegistry();
      registry.plugins.push(createPluginRecord({ id: original.pluginId }));
      registry.gatewayAccessPolicies.push({
        pluginId: original.pluginId,
        source: "fixture",
        policy: { authorize: () => grant, resume },
      });
      const previous = captureActivePluginRegistrySnapshot();
      stageActivePluginRegistry(registry, null, "default");
      try {
        const profile = ensureProfileForEmail("access@example.test");
        const fixture = await createOperatorRecoveryFixture({
          stateDir: state.stateDir,
          context: createContext(),
          profileId: profile.id,
          config: {},
          gatewayAccessGrant: original,
        });
        permitted = false;
        await expect(fixture.restore()).rejects.toThrow("Gateway access is not active");
        permitted = true;
        const restored = expectDefined(await fixture.restore(), "restored original grant");
        try {
          expect(restored.authority.gatewayAccessGrant).toEqual(original);
          expect(resume).toHaveBeenCalledWith(
            expect.objectContaining({ grantId: original.grantId }),
          );
          controller.abort(new Error("Original grant revoked"));
          expect(restored.authority.signal?.aborted).toBe(true);
          expect(restored.authority.assertCurrent).toThrow();
        } finally {
          restored.release();
        }
      } finally {
        restoreActivePluginRegistrySnapshot(previous);
      }
    });
  });
  it("re-admits durable input after the original authority closes without exporting credentials", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      setRuntimeConfigSnapshot({});
      const profile = ensureProfileForEmail("recovery@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
        device: pairedOperator(),
        scopes: ["operator.write"],
      });
      expect(fixture.sourceAuthority.assertCurrent).toThrow("no longer active");
      expect(fixture.entry.restartRecoveryOperatorSource?.snapshot).toMatchObject({
        controlUiAdmin: false,
        sourceIngress: "control-ui",
      });
      const serialized = JSON.stringify(fixture.entry.restartRecoveryOperatorSource);
      expect(serialized).not.toContain("fixture-device-token");
      expect(serialized).not.toContain("fixture-signature");
      const admitted = await persistSessionTranscriptTurn(fixture.target, {
        expectedSessionId: fixture.target.sessionId,
        expectedSessionState: buildRestartRecoveryExpectedState(fixture.entry),
        messages: [
          {
            idempotencyLookup: "scan",
            message: {
              role: "user",
              content: "Continue accepted work",
              idempotencyKey: "serialized-source:user",
              timestamp: 1,
            },
          },
        ],
        updateMode: "none",
      });
      expect(admitted.appendedCount).toBe(1);
      expect(admitted.rejectedReason).toBeUndefined();
      const restored = expectDefined(await fixture.restore(), "restored operator source");
      try {
        expect(restored.authority.scopes).toEqual(["operator.write"]);
        expect(restored.authority.profileId).toBe(profile.id);
        expect(restored.authority.assertCurrent).not.toThrow();
        const retain = expectDefined(restored.authority.retain, "recovery retention")();
        restored.release();
        expect(restored.authority.assertCurrent).not.toThrow();
        retain();
        expect(restored.authority.assertCurrent).toThrow("no longer active");
      } finally {
        restored.release();
      }
    });
  });

  it.each([
    "legacy claim",
    "different source turn",
    "different session",
    "different lifecycle",
  ] as const)("never reconstructs authorization from attribution for a %s", async (change) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("claim@example.test");
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
      });
      const changed = { ...fixture.entry };
      if (change === "legacy claim") {
        changed.restartRecoveryOperatorSource = undefined;
      } else if (change === "different source turn") {
        changed.restartRecoveryDeliverySourceRunId = "another-turn";
      } else if (change === "different session") {
        changed.sessionId = "replacement-session";
      } else {
        changed.lifecycleRevision = "replacement-lifecycle";
      }
      await replaceSessionEntry(fixture.target, changed);
      if (change === "legacy claim") {
        await expect(fixture.restore()).resolves.toBeUndefined();
      } else {
        await expect(fixture.restore()).rejects.toThrow(/no longer owns|no longer active/);
      }
    });
  });

  it.each([
    "profile merge",
    "role changed",
    "device removed",
    "token revoked",
    "token rotated",
    "token scopes narrowed",
    "approval scopes narrowed",
  ] as const)("refuses current-policy re-admission after %s", async (change) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("policy@example.test");
      const device = pairedOperator();
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: {},
        device,
      });
      if (change === "profile merge") {
        const target = ensureProfileForEmail("merged@example.test");
        linkEmail("policy@example.test", target.id);
      } else if (change === "role changed") {
        setUserProfileRole(profile.id, "new-role");
      } else {
        const token = expectDefined(device.tokens?.operator, "operator token");
        if (change === "token revoked") {
          token.revokedAtMs = 2;
        }
        if (change === "token rotated") {
          token.token = "rotated-fixture-token";
        }
        if (change === "token scopes narrowed") {
          token.scopes = ["operator.read"];
        }
        if (change === "approval scopes narrowed") {
          device.approvedScopes = ["operator.read"];
        }
        persistDevicePairingStoreState(
          {
            pendingById: {},
            pairedByDeviceId: change === "device removed" ? {} : { [device.deviceId]: device },
          },
          state.stateDir,
          "both",
        );
      }
      await expect(fixture.restore()).rejects.toThrow(
        /Restart recovery operator|user profile not found/,
      );
    });
  });

  it.each(["claim retired", "device revoked", "session replaced"] as const)(
    "fences retained recovered authority after %s",
    async (change) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const profile = ensureProfileForEmail("retained@example.test");
        const fixture = await createOperatorRecoveryFixture({
          stateDir: state.stateDir,
          context: createContext(),
          profileId: profile.id,
          config: {},
          device: pairedOperator(),
        });
        const restored = expectDefined(await fixture.restore(), "restored operator source");
        try {
          if (change === "claim retired") {
            fixture.retire();
          }
          if (change === "device revoked") {
            invalidateGatewayDeviceRevocation(fixture.context, "recovery-device", "operator");
          }
          if (change === "session replaced") {
            await replaceSessionEntry(fixture.target, {
              ...fixture.entry,
              sessionId: "replacement-session",
            });
          }
          expect(restored.authority.assertCurrent).toThrow();
        } finally {
          restored.release();
        }
      });
    },
  );

  it("intersects the original model ceiling with current policy without widening the admitted role", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const profile = ensureProfileForEmail("model@example.test");
      setUserProfileRole(profile.id, "operator");
      const cfg: OpenClawConfig = {
        agents: { defaults: { model: "fixture/a" } },
        gateway: {
          roles: {
            definitions: {
              operator: {
                sessions: { others: "none" },
                agents: ["main"],
                scopes: ["operator.admin"],
                sandbox: "required",
                accessPolicyPlugin: undefined,
                modelPolicy: { allow: ["fixture/a", "fixture/b"] },
              },
            },
          },
        },
      };
      const fixture = await createOperatorRecoveryFixture({
        stateDir: state.stateDir,
        context: createContext(),
        profileId: profile.id,
        config: cfg,
      });
      const current = structuredClone(cfg);
      expectDefined(current.gateway?.roles?.definitions.operator, "operator role").modelPolicy = {
        allow: ["fixture/b", "fixture/c"],
      };
      fixture.setConfig(current);
      const restored = expectDefined(await fixture.restore(), "model-restricted recovery");
      try {
        expect(restored.authority.rolePolicy).toMatchObject({
          agents: ["main"],
          sandboxRequired: true,
          sessionAccessCap: "none",
        });
        expect(restored.authority.modelPolicy?.allows({ provider: "fixture", model: "a" })).toBe(
          false,
        );
        expect(restored.authority.modelPolicy?.allows({ provider: "fixture", model: "b" })).toBe(
          true,
        );
        expect(restored.authority.modelPolicy?.allows({ provider: "fixture", model: "c" })).toBe(
          false,
        );
        const widened = structuredClone(current);
        expectDefined(widened.gateway?.roles?.definitions.operator, "operator role").modelPolicy = {
          allow: ["fixture/a", "fixture/b", "fixture/c"],
        };
        fixture.setConfig(widened);
        expect(restored.authority.modelPolicy?.models).toEqual([
          { provider: "fixture", model: "b" },
        ]);
      } finally {
        restored.release();
      }
    });
  });
});
