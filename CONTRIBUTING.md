# Contributing

Thank you for contributing to this project! Please follow these guidelines.

## Pull Request Process

### Approval and Merge Policy

Pull requests are approved for merge through one of two mechanisms:

#### 1. Exact-Head Review Approval (Recommended)

A PR is approved for merge when:
- An **exact-head APPROVED review** exists from a reviewer who:
  - Is NOT the PR author
  - Has write access to the repository
  - Reviewed the current HEAD commit
- The reviewer has no later **CHANGES_REQUESTED** or **DISMISSED** reviews

This approval **overrides** any `needs-human` verdict line in the PR body.

Examples:
- ✅ Approved by `reviewer1` on commit `abc123` (current HEAD)
- ❌ Approved by author (self-approval doesn't count)
- ❌ Approved by `reviewer1` on commit `old123`, but HEAD is now `abc123`
- ❌ Approved by `reviewer1`, but later `reviewer2` requested changes

#### 2. Body Verdict Line

PRs can include a verdict line in the body to indicate merge status:

```
gajae.pr-review-verdict.v1 <status> reviewer-id:<id>
```

Supported statuses:

- **`merge-approved`**: PR is approved for merge (e.g., by an automated system or tool)
  - Blocks all other approval mechanisms
  - Always grants merge approval
  
- **`merge-blocked`**: PR is blocked from merge (e.g., by a maintainer decision)
  - Blocks merge unconditionally
  - Overrides any review approvals
  
- **`needs-human`**: PR requires human review (default for template-generated lines)
  - Requires an exact-head APPROVED review from a non-author with write access
  - Can be overridden by review approval

### Automatic Merge Verification

The CI workflow will verify merge approval on every push and PR update:

```
bun run scripts/verify-pr-verdict.ts <pr-data-path>
```

- **Exit code 0**: PR is approved for merge
- **Exit code 1**: PR is pending or blocked

### Policy Summary

| Scenario | Result |
|----------|--------|
| Exact-head approval + no blocking reviews | ✅ Approved |
| Exact-head approval + CHANGES_REQUESTED later | ❌ Blocked |
| Exact-head approval + needs-human line | ✅ Approved (line overridden) |
| Stale-head approval + needs-human line | ❌ Pending (approval too old) |
| Author self-approval | ❌ Pending (self-approval doesn't count) |
| No approval + no verdict line | ❌ Pending |
| No approval + merge-approved line | ✅ Approved |
| No approval + merge-blocked line | ❌ Blocked |

## Code Guidelines

- Follow the repository's code style (configured in `biome.json`)
- Run `bun run test` locally before pushing
- Run `bunx biome ci packages/` to check formatting
- Run `bunx tsc --noEmit -p tsconfig.json` to verify types

## Commit Messages

- Use clear, concise commit messages
- Reference related issues (e.g., "fixes #123")
- Separate subject line from body with a blank line

## Testing

- Add tests for new functionality
- Ensure all tests pass before submitting your PR
- Include integration tests for significant changes

## Questions?

If you have questions about the contribution process, please open an issue or reach out to the maintainers.
