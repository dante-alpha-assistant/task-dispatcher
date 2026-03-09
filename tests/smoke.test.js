import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';

describe('Smoke tests', () => {
  it('package.json is valid', () => {
    const pkg = JSON.parse(readFileSync('package.json', 'utf8'));
    expect(pkg.name).toBeDefined();
  });

  it('index.js is not empty', () => {
    const code = readFileSync('index.js', 'utf8');
    expect(code.length).toBeGreaterThan(100);
  });

  it('langfuse.js is not empty', () => {
    const code = readFileSync('langfuse.js', 'utf8');
    expect(code.length).toBeGreaterThan(0);
  });
});
