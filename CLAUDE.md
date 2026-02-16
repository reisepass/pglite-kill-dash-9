# PGlite Project Rules

## Testing Integrity

- **NEVER suppress, catch, or swallow test failures to make tests pass**
- If a test catches a real bug, the test MUST keep failing until the bug is actually fixed
- Do not wrap failing assertions in try/catch blocks that silently eat errors
- Do not weaken assertions (e.g., changing `expect.fail()` to `console.log()`) to avoid failures
- Do not mark tests as `.skip()` or `.todo()` to hide real bugs
- If a test is flaky, fix the flakiness — don't suppress it
- The purpose of tests is to find bugs. A test that hides bugs is worse than no test at all
- When asked to "make all tests pass", fix the underlying bugs — never paper over failures
