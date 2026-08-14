import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { modelsFromKiroModelListResponse } from "./KiroProvider.ts";

describe("modelsFromKiroModelListResponse", () => {
  it.effect("turns Kiro CLI model output into selectable provider models", () =>
    Effect.gen(function* () {
      const models = yield* modelsFromKiroModelListResponse(`{
        "models": [
          { "model_id": "auto", "model_name": "Auto" },
          {
            "model_id": "claude-sonnet-4.5",
            "model_name": "Claude Sonnet 4.5",
            "description": "A fast model for everyday coding."
          },
          { "model_id": "auto", "model_name": "Duplicate" }
        ]
      }`);

      expect(models.map((model) => [model.slug, model.name])).toEqual([
        ["auto", "Auto"],
        ["claude-sonnet-4.5", "Claude Sonnet 4.5"],
      ]);
    }),
  );
});
