# Cann-o-Call License Decision

**Decision status:** `SELECTED_PENDING_FINALIZATION_AND_REVIEW`  
**Selected model:** Business Source License 1.1 (BSL 1.1) + Cann-o-Call Small-Business Production Use Grant.

This is a release-policy decision, not a claim that the final legal license has already been adopted.

## Selected policy direction

The current policy draft establishes the following intended model:

- source remains publicly inspectable under BSL 1.1 terms;
- individuals/non-production evaluators receive the standard BSL permissions;
- internal Production use is additionally granted to an eligible small organization only while the organization and affiliates together have **fewer than 50 FTE employees AND less than USD $5,000,000 gross annual revenue**;
- exceeding either threshold requires a separate commercial license for continued Production use;
- SaaS/hosted-service/resale/managed-service/white-label use principally based on Cann-o-Call is outside the free small-business Production grant;
- copyright/IP ownership remains with the copyright holder(s);
- each BSL-covered version eventually converts under its Change Date/Change License terms.

## Fields intentionally unresolved

Do not fabricate these values. They must be completed before a public release:

1. Licensor / copyright holder / legal entity.
2. Copyright year/name presentation.
3. Exact Licensed Work/version parameter.
4. Exact Change Date.
5. Exact Change License.
6. Commercial licensing email or website.
7. Final affiliate definition and threshold transition/grace wording.
8. Treatment of MSPs/consultants, nonprofits, education, and contributors.
9. Legal review of the Additional Use Grant and Change License compatibility.

## Release rule

### Internal/private RC

May be packaged for evaluation with the draft preserved under a clearly marked `review/` path. Do not present the draft as an adopted `LICENSE`.

### Public RC/release

Fail closed until:

- an adopted `LICENSE` exists;
- no placeholder fields remain;
- `LICENSING.md` explains the model accurately;
- `NOTICE` contains the intended creator/copyright attribution;
- the BSL parameter block and Additional Use Grant have received appropriate review.

Before the Change Date, describe the project as **source-available**, not OSI Open Source.

## Why this is the selected model

It matches the stated product policy: free internal use for genuine small operators while requiring commercial terms from enterprise-scale organizations and commercial hosted/resale offerings, without transferring ownership of the underlying intellectual property.
