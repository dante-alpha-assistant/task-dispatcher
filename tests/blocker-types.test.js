import { describe, it, expect } from "vitest";

const VALID_BLOCKER_TYPES = [
  "missing_credential",
  "missing_config",
  "ambiguous_requirement",
  "permission_denied",
  "external_dependency",
  "infrastructure",
  "human_decision",
];

const VALID_INPUT_TYPES = ["text", "password", "select", "boolean", "url"];

function validateBlocker(blocker) {
  const errors = [];
  if (!blocker.type || !VALID_BLOCKER_TYPES.includes(blocker.type))
    errors.push(`Invalid type: ${blocker.type}`);
  if (!blocker.title) errors.push("Missing title");
  if (!blocker.description) errors.push("Missing description");
  if (!Array.isArray(blocker.required_inputs))
    errors.push("required_inputs must be an array");
  else {
    blocker.required_inputs.forEach((input, i) => {
      if (!input.key) errors.push(`Input ${i}: missing key`);
      if (!input.label) errors.push(`Input ${i}: missing label`);
      if (!input.type) errors.push(`Input ${i}: missing type`);
    });
  }
  return errors;
}

describe("Structured Blocker Types", () => {
  it("should define at least 5 blocker types", () => {
    expect(VALID_BLOCKER_TYPES.length).toBeGreaterThanOrEqual(5);
  });

  it("should have all 7 required blocker types", () => {
    for (const type of [
      "missing_credential", "missing_config", "ambiguous_requirement",
      "permission_denied", "external_dependency", "infrastructure", "human_decision",
    ]) {
      expect(VALID_BLOCKER_TYPES).toContain(type);
    }
  });

  it("should validate a complete blocker metadata object", () => {
    const blocker = {
      type: "missing_credential",
      title: "Langfuse API keys not configured",
      description: "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are empty",
      required_inputs: [
        { key: "LANGFUSE_PUBLIC_KEY", label: "Langfuse Public Key", type: "text", placeholder: "pk-lf-..." },
        { key: "LANGFUSE_SECRET_KEY", label: "Langfuse Secret Key", type: "password", placeholder: "sk-lf-..." },
      ],
      suggested_action: "Sign up at cloud.langfuse.com and paste your project keys",
    };
    expect(validateBlocker(blocker)).toEqual([]);
  });

  it("should reject blocker with invalid type", () => {
    const blocker = { type: "invalid_type", title: "T", description: "D", required_inputs: [] };
    const errors = validateBlocker(blocker);
    expect(errors.some(e => e.includes("Invalid type"))).toBe(true);
  });

  it("should reject blocker missing title", () => {
    const errors = validateBlocker({ type: "infrastructure", description: "D", required_inputs: [] });
    expect(errors.some(e => e.includes("Missing title"))).toBe(true);
  });

  it("should reject required_inputs without key/label/type", () => {
    const blocker = {
      type: "missing_config",
      title: "T", description: "D",
      required_inputs: [{ key: "X" }], // missing label and type
    };
    const errors = validateBlocker(blocker);
    expect(errors.length).toBeGreaterThan(0);
  });

  it("should accept all 7 blocker types individually", () => {
    for (const type of VALID_BLOCKER_TYPES) {
      const blocker = { type, title: `Test ${type}`, description: "Desc", required_inputs: [] };
      expect(validateBlocker(blocker)).toEqual([]);
    }
  });
});
