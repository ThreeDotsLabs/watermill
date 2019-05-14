# Release procedure

1. [ ] - generate clean go.mod: `make generate_gomod`
2. [ ] - commit && push to master
3. [ ] - update and validate examples: `make validate_examples`
4. [ ] - update missing documentation
5. [ ] - commit && push to master
6. [ ] - add breaking changes to `UPGRADE-[new-version].md`
7. [ ] - commit && push to master
8. [ ] - wait for `master` CI build
9. [ ] - [add release in GitHub](https://github.com/ThreeDotsLabs/watermill/releases)
