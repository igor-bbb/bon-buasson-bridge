# VECTRA — Platform Governance Release

Deploy package for VECTRA platform governance and Release Brief synchronization standard.

## Main changes

- Base platform architecture is treated as stabilized for subsequent engineering work.
- Release Brief now contains a mandatory **Instruction Synchronization** section.
- Release Brief supports two explicit outcomes:
  - `Изменение инструкции VECTRA не требуется.`
  - `Требуется обновление инструкции VECTRA.`
- Release Brief parser accepts instruction synchronization metadata from structured payloads.
- Engineering changes remain separated from Product Team Assistant ownership of product behavior and VECTRA instruction.
- Instruction was not changed in this release.

## Release artifact standard

Every release must include:

1. Release Brief.
2. Deploy-ready ZIP.
3. Local verification results.
4. Known limitations.
5. Instruction Synchronization status.

## Instruction Synchronization

Instruction update status for this release:

**Изменение инструкции VECTRA не требуется.**
