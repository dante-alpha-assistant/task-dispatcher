import { describe, it, expect } from "vitest";
import { detectBlockerPattern, buildBlockerMetadata, getDefaultPatterns } from "../blocker-patterns.js";

describe("blocker-patterns", () => {
  it("has at least 5 default patterns", () => {
    expect(getDefaultPatterns().length).toBeGreaterThanOrEqual(5);
  });

  describe("missing_credential", () => {
    it("detects missing API key", () => {
      const result = detectBlockerPattern("Error: GH_TOKEN is not configured");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("missing_credential");
    });

    it("detects empty token", () => {
      const result = detectBlockerPattern("Auth failed: API key is empty, cannot proceed");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("missing_credential");
    });

    it("detects missing env var via regex", () => {
      const result = detectBlockerPattern("SECRET_KEY is not set in environment");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("missing_credential");
    });
  });

  describe("permission_denied", () => {
    it("detects 401 status", () => {
      const result = detectBlockerPattern("GitHub API returned HTTP 401 for repo access");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("permission_denied");
    });

    it("detects 403 forbidden", () => {
      const result = detectBlockerPattern("Request failed: 403 Forbidden");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("permission_denied");
    });

    it("detects unauthorized keyword", () => {
      const result = detectBlockerPattern("kubectl apply failed: not authorized to create pods in namespace agents");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("permission_denied");
    });
  });

  describe("infrastructure", () => {
    it("detects ECONNREFUSED", () => {
      const result = detectBlockerPattern("connect ECONNREFUSED 127.0.0.1:5432");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("infrastructure");
    });

    it("detects 502 bad gateway", () => {
      const result = detectBlockerPattern("Supabase returned HTTP 502 Bad Gateway");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("infrastructure");
    });

    it("detects 503 service unavailable", () => {
      const result = detectBlockerPattern("ArgoCD server returned 503 — service unavailable");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("infrastructure");
    });
  });

  describe("ambiguous_requirement", () => {
    it("detects need clarification", () => {
      const result = detectBlockerPattern("Task description is unclear — need clarification on which API endpoint to modify");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("ambiguous_requirement");
    });

    it("detects which approach", () => {
      const result = detectBlockerPattern("Multiple approaches possible: which approach should I use for the auth flow?");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("ambiguous_requirement");
    });
  });

  describe("resource_limit", () => {
    it("detects OOMKilled", () => {
      const result = detectBlockerPattern("Pod terminated: OOMKilled — exceeded 2Gi memory limit");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("resource_limit");
    });

    it("detects disk full", () => {
      const result = detectBlockerPattern("Error: no space left on device");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("resource_limit");
    });

    it("detects rate limit / 429", () => {
      const result = detectBlockerPattern("OpenRouter API: HTTP 429 Too Many Requests — rate limit exceeded");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("resource_limit");
    });
  });

  describe("dependency_missing", () => {
    it("detects command not found", () => {
      const result = detectBlockerPattern("bash: kubectl: command not found");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("dependency_missing");
    });

    it("detects cannot find module", () => {
      const result = detectBlockerPattern("Error: Cannot find module '@supabase/supabase-js'");
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("dependency_missing");
    });
  });

  describe("no match", () => {
    it("returns null for unrelated errors", () => {
      const result = detectBlockerPattern("TypeError: Cannot read property 'map' of undefined");
      expect(result).toBeNull();
    });

    it("returns null for empty string", () => {
      expect(detectBlockerPattern("")).toBeNull();
    });

    it("returns null for null/undefined", () => {
      expect(detectBlockerPattern(null)).toBeNull();
      expect(detectBlockerPattern(undefined)).toBeNull();
    });
  });

  describe("buildBlockerMetadata", () => {
    it("builds structured metadata from detection", () => {
      const detection = detectBlockerPattern("Auth failed: API key is empty");
      expect(detection).not.toBeNull();

      const meta = buildBlockerMetadata(detection);
      expect(meta.type).toBe("missing_credential");
      expect(meta.title).toBe("Missing or invalid credentials");
      expect(meta.auto_detected).toBe(true);
      expect(meta.detected_at).toBeTruthy();
      expect(meta.required_inputs).toBeInstanceOf(Array);
      expect(meta.required_inputs.length).toBeGreaterThan(0);
      expect(meta.suggested_action).toBeTruthy();
      expect(meta.description).toContain("Auto-detected from error:");
      expect(meta.matched_keywords).toBeInstanceOf(Array);
    });
  });

  describe("custom patterns", () => {
    it("accepts custom pattern list", () => {
      const customPatterns = [
        {
          type: "custom_error",
          title: "Custom error",
          keywords: ["xyzzy"],
          required_inputs: [],
          suggested_action: "Do the thing",
        },
      ];
      const result = detectBlockerPattern("Task failed with xyzzy error", customPatterns);
      expect(result).not.toBeNull();
      expect(result.pattern.type).toBe("custom_error");
    });
  });
});
