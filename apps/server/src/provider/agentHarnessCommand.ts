/** Resolve the Agent Harness executable used by T3 Code. */
export const DEFAULT_AGENT_HARNESS_BINARY = "agent-harness-rs";

/**
 * Migrate the pre-Rust default in memory. Absolute/custom paths remain
 * explicit so only the legacy bare command is redirected.
 */
export function resolveAgentHarnessBinaryPath(binaryPath: string | undefined): string {
  const value = binaryPath?.trim();
  return !value || value === "agent-harness" ? DEFAULT_AGENT_HARNESS_BINARY : value;
}
