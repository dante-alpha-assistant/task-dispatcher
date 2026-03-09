import { describe, it, expect } from "vitest";

// Blocker type validation — ensures structured blocker metadata matches expected schema
const VALID_BLOCKER_TYPES = [
  "missing_credential",
  "missing_config",
  "ambiguous_requirement",
  "permission_denied",
  "external_dependency",
  "infrastructure",
  "human_decision",
];

const VALID_INPUT_TYPES = ["text", "password", "select", "url"];

function validateBlockerMetadata(metadata) {
  const errors = [];

  if (!metadata || typeof metadata !== "object") {
    return ["blocker_metadata must be an object"];
  }

  if (!metadata.type || !VALID_BLOCKER_TYPES.includes(metadata.type)) {
    errors.push(`Invalid blocker type: "${metadata.type}". Must be one of: ${VALID_BLOCKER_TYPES.join(", ")}`);
  }

  if (!metadata.title || typeof metadata.title !== "string") {
    errors.push("blocker_metadata.title is required and must be a string");
  }

  if (!metadata.description || typeof metadata.description !== "string") {
    errors.push("blocker_metadata.description is required and must be a string");
  }

  if (!Array.isArray(metadata.required_inputs)) {
    errors.push("blocker_metadata.required_inputs must be an array");
  } else {
    for (const [i, input] of metadata.required_inputs.entries()) {
      if (!input.key) errors.push(`required_inputs[${i}].key is required`);
      if (!input.label) errors.push(`required_inputs[${i}].label is required`);
      if (!input.type || !VALID_INPUT_TYPES.includes(input.type)) {
        errors.push(`required_inputs[${i}].type must be one of: ${VALID_INPUT_TYPES.join(", ")}`);
      }
    }
  }

  return errors;
}

describe("Blocker Metadata Validation", () => {
  it("accepts valid missing_credential blocker", () => {
    const metadata = {
      type: "missing_credential",
      title: "Langfuse API keys not configured",
      description: "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are empty in dispatcher env",
      required_inputs: [
        { key: "LANGFUSE_PUBLIC_KEY", label: "Langfuse Public Key", type: "text", placeholder: "pk-lf-..." },
        { key: "LANGFUSE_SECRET_KEY", label: "Langfuse Secret Key", type: "password", placeholder: "sk-lf-..." },
      ],
      suggested_action: "Sign up at cloud.langfuse.com and paste your project keys",
    };
    expect(validateBlockerMetadata(metadata)).toEqual([]);
  });

  it("accepts valid ambiguous_requirement blocker", () => {
    const metadata = {
      type: "ambiguous_requirement",
      title: "Unclear which API to integrate",
      description: "Task says 'add payment support' but doesn't specify Stripe vs PayPal",
      required_inputs: [],
      suggested_action: "Clarify which payment provider to use",
    };
    expect(validateBlockerMetadata(metadata)).toEqual([]);
  });

  it("accepts valid infrastructure blocker", () => {
    const metadata = {
      type: "infrastructure",
      title: "Database unreachable",
      description: "PostgreSQL connection refused on port 5432",
      required_inputs: [
        { key: "DATABASE_URL", label: "Database Connection String", type: "url", placeholder: "postgresql://..." },
      ],
      suggested_action: "Check if the database pod is running: kubectl get pods -n infra",
    };
    expect(validateBlockerMetadata(metadata)).toEqual([]);
  });

  it("accepts valid human_decision blocker", () => {
    const metadata = {
      type: "human_decision",
      title: "REST vs GraphQL for new API",
      description: "The task requires a new API but both REST and GraphQL are valid approaches with different tradeoffs",
      required_inputs: [
        { key: "API_STYLE", label: "API Style", type: "select", placeholder: "rest|graphql" },
      ],
      suggested_action: "Choose REST for simplicity or GraphQL for flexible querying",
    };
    expect(validateBlockerMetadata(metadata)).toEqual([]);
  });

  it("accepts valid permission_denied blocker", () => {
    const metadata = {
      type: "permission_denied",
      title: "Cannot access production namespace",
      description: "kubectl apply -n prod returns 403 Forbidden",
      required_inputs: [],
      suggested_action: "Grant the agent's service account access to the prod namespace",
    };
    expect(validateBlockerMetadata(metadata)).toEqual([]);
  });

  it("rejects invalid blocker type", () => {
    const errors = validateBlockerMetadata({ type: "unknown_type", title: "x", description: "y", required_inputs: [] });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain("Invalid blocker type");
  });

  it("rejects missing title", () => {
    const errors = validateBlockerMetadata({ type: "infrastructure", description: "y", required_inputs: [] });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors.some(e => e.includes("title"))).toBe(true);
  });

  it("rejects missing required_inputs array", () => {
    const errors = validateBlockerMetadata({ type: "infrastructure", title: "x", description: "y" });
    expect(errors.some(e => e.includes("required_inputs must be an array"))).toBe(true);
  });

  it("rejects invalid input type in required_inputs", () => {
    const errors = validateBlockerMetadata({
      type: "missing_config",
      title: "x",
      description: "y",
      required_inputs: [{ key: "FOO", label: "Foo", type: "number" }],
    });
    expect(errors.some(e => e.includes("required_inputs[0].type"))).toBe(true);
  });

  it("rejects null metadata", () => {
    const errors = validateBlockerMetadata(null);
    expect(errors).toEqual(["blocker_metadata must be an object"]);
  });
});

// Export for potential reuse
export { validateBlockerMetadata, VALID_BLOCKER_TYPES, VALID_INPUT_TYPES };
