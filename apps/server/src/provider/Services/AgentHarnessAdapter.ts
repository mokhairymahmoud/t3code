/** @module provider/Services/AgentHarnessAdapter */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface AgentHarnessAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
