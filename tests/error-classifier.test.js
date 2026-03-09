import { describe, it, expect } from "vitest";
import { classifyError, getErrorCategories } from "../error-classifier.js";

describe("classifyError", () => {
  it("classifies merge conflicts", () => {
    expect(classifyError("CONFLICT: Merge conflict in src/index.js").category).toBe("merge_conflict");
    expect(classifyError("PR is not mergeable, needs rebase").category).toBe("merge_conflict");
  });

  it("classifies CI failures", () => {
    expect(classifyError("CI check failed: tests returned exit code 1").category).toBe("ci_failure");
    expect(classifyError("Build failed: npm run build").category).toBe("ci_failure");
    expect(classifyError("jest test suite failed").category).toBe("ci_failure");
  });

  it("classifies timeouts", () => {
    expect(classifyError("Task timed out after 30 minutes").category).toBe("timeout");
    expect(classifyError("absolute timeout exceeded").category).toBe("timeout");
    expect(classifyError("Operation took too long").category).toBe("timeout");
  });

  it("classifies session lost", () => {
    expect(classifyError("Agent session lost during execution").category).toBe("session_lost");
    expect(classifyError("ECONNRESET: connection was reset").category).toBe("session_lost");
    expect(classifyError("Pod restarted during task execution").category).toBe("session_lost");
  });

  it("classifies QA rejections", () => {
    expect(classifyError("QA failed: code review found issues").category).toBe("qa_rejection");
    expect(classifyError("QA rejected the implementation").category).toBe("qa_rejection");
  });

  it("classifies auth errors", () => {
    expect(classifyError("401 Unauthorized: invalid token").category).toBe("auth_error");
    expect(classifyError("Permission denied: forbidden").category).toBe("auth_error");
    expect(classifyError("GH_TOKEN expired").category).toBe("auth_error");
  });

  it("classifies resource errors", () => {
    expect(classifyError("Container OOMKilled").category).toBe("resource_error");
    expect(classifyError("No space left on device (ENOSPC)").category).toBe("resource_error");
    expect(classifyError("Out of memory error").category).toBe("resource_error");
  });

  it("returns unknown for unrecognized errors", () => {
    expect(classifyError("Something weird happened").category).toBe("unknown");
    expect(classifyError("").category).toBe("unknown");
    expect(classifyError(null).category).toBe("unknown");
    expect(classifyError(undefined).category).toBe("unknown");
  });

  it("returns correct metadata", () => {
    const result = classifyError("timed out");
    expect(result.retriable).toBe(true);
    expect(result.action).toBe("auto_retry");
    expect(result.label).toBe("Timeout");
    expect(result.color).toBeTruthy();
  });
});

describe("getErrorCategories", () => {
  it("returns all categories with metadata", () => {
    const categories = getErrorCategories();
    expect(categories.length).toBeGreaterThanOrEqual(8);
    const names = categories.map(c => c.category);
    expect(names).toContain("merge_conflict");
    expect(names).toContain("ci_failure");
    expect(names).toContain("timeout");
    expect(names).toContain("session_lost");
    expect(names).toContain("qa_rejection");
    expect(names).toContain("auth_error");
    expect(names).toContain("resource_error");
    expect(names).toContain("unknown");
    categories.forEach(c => {
      expect(c).toHaveProperty("label");
      expect(c).toHaveProperty("color");
      expect(c).toHaveProperty("action");
    });
  });
});
