# Repository conventions

## Language

Write all content intended for version control in English, including source code,
comments, docstrings, documentation and analysis reports.

## Documentation and private working material

- Use `docs/` for project documentation and useful, shareable analysis, including
  benchmark reports and their supporting results.
- Keep historical experiments in the ignored `investigation/` directory.
- Internal ADRs are working analysis for this project, not public documentation.
- Promote useful findings into reviewed English documentation under `docs/`;
  do not publish private plans as documentation or link public documents to
  ignored local files.
- Use repository-relative links in versioned documentation. Keep benchmark
  measurements unchanged when organizing or translating their presentation.
