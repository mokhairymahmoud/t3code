import * as Effect from "effect/Effect";

import { TextGenerationError } from "@t3tools/contracts";
import type * as TextGeneration from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: "Agent Harness does not yet support text generation.",
    }),
  );

export const makeAgentHarnessTextGeneration = Effect.sync(
  (): TextGeneration.TextGeneration["Service"] => ({
    generateCommitMessage: () => unsupported("generateCommitMessage"),
    generatePrContent: () => unsupported("generatePrContent"),
    generateBranchName: () => unsupported("generateBranchName"),
    generateThreadTitle: () => unsupported("generateThreadTitle"),
  }),
);
