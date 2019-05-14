# Release procedure

1. Generate clean go.mod: `make generate_gomod`
2. Update and validate examples: `make validate_examples`
3. Update missing documentation
4. Check snippets in documentation (sometimes `first_line_contains` or `last_line_contains` can change position and load too much)
5. Add breaking changes to `UPGRADE-[new-version].md`
6. [Add release in GitHub](https://github.com/ThreeDotsLabs/watermill/releases)
