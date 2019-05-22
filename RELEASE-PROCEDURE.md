# Release procedure

1. Generate clean go.mod: `make generate_gomod`
2. Push to master
3. Update and validate examples: `make validate_examples`
4. Update missing documentation
5. Check snippets in documentation (sometimes `first_line_contains` or `last_line_contains` can change position and load too much)
6. Add breaking changes to `UPGRADE-[new-version].md`
7. Push to master
8. [Add release in GitHub](https://github.com/ThreeDotsLabs/watermill/releases)
