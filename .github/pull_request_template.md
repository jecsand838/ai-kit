## Linked issue (required)

<!-- Link the issue this PR addresses. Use "Closes #123" when merging to main
     should close it. For partial work, reference the issue and link it in the
     Development sidebar without claiming that it is complete. -->

## Changes and acceptance criteria

<!-- Explain the problem, resulting behavior, and how the linked issue's
     acceptance criteria are met. Mention compatibility or migration effects. -->

## Validation evidence

<!-- Record commands actually run against the current change. Include the tested
     commit, or base commit plus a description of uncommitted changes. Sanitize
     logs before sharing; never include keys, auth files, private data, or populated
     env files. Record failures and missing prerequisites, not just successful runs. -->

| Command | Crate / backend / features | Tested revision | Result | Sanitized receipt or log | Exclusions or blockers |
| --- | --- | --- | --- | --- | --- |
| | | | | | |

GitHub CI does not establish strict-live coverage. Follow the owning crate's
gates and the [contribution guide](https://github.com/jecsand838/ai-kit/blob/main/CONTRIBUTING.md).
Qualified exclusions are not full workspace release qualification.

## Acceptance checklist

The linked issue and this checklist are review requirements; this template does
not enforce them automatically. Check only completed items. For prose/template-only
changes, leave inapplicable behavioral-test and strict-live boxes unchecked and
explain each N/A below. Applicable documentation checks still run. Missing
credentials or failed required tests are blockers, not N/A.

- [ ] I linked the issue and addressed the stated acceptance criteria.
- [ ] I followed the root and affected crate [operating contracts](https://github.com/jecsand838/ai-kit/blob/main/AGENTS.md).
- [ ] I updated affected owning docs, examples, and rustdoc, or explained why no documentation update is needed.
- [ ] I added or strengthened meaningful tests for changed behavior and recorded required pre-fix failure evidence.
- [ ] I ran the applicable deterministic and documentation checks and recorded their results above.
- [ ] I certify that all relevant strict-live suites passed locally against real services, using my own authorized keys/accounts where required; commands, backend/feature coverage, results, and any explicitly permitted exclusions are recorded above.
- [ ] The actual diff received independent subagent review as required by the operating contract, and findings were resolved.
- [ ] The diff and shared evidence contain no secrets, populated env files, or private test data.

## N/A explanations and outstanding work

<!-- Explain each inapplicable test item for a prose/template-only change. List
     unresolved failures, prerequisites, or review findings separately; do not
     check the corresponding acceptance item until it is complete. -->
