# Cann-o-Call Licensing Policy Draft
## BSL 1.1 + Small-Business Production Use Grant

**Status:** Working draft for project-source review only. Not yet adopted as the project license.  
**Project:** Cann-o-Call  
**Draft purpose:** Preserve public source access and free use for individuals/small organizations while requiring a separate commercial license for enterprise-scale production use and commercial hosting/resale.  
**Legal note:** This is a project-policy draft, not legal advice. Before adoption, the final parameter block and Additional Use Grant should be reviewed by qualified counsel.

---

## 1. Licensing Goal

Cann-o-Call should remain publicly inspectable and modifiable while preserving the copyright holder's ownership and the ability to require commercial licensing from organizations receiving substantial commercial value.

The intended policy is:

- Individuals, hobbyists, researchers, students, and non-production evaluators can use the source without purchasing a commercial license, subject to the standard BSL 1.1 terms.
- Small organizations may use Cann-o-Call internally in production without charge while they remain below defined employee and revenue thresholds.
- Large/enterprise organizations must obtain a separate commercial license for production use.
- Offering Cann-o-Call itself, or substantially equivalent hosted/managed functionality based on it, as a third-party commercial service is not included in the free small-business production grant.
- Copyright and other intellectual-property ownership remain with the copyright holder(s); the license grants permissions rather than transferring ownership.
- Each BSL-covered version eventually converts to the selected Open Source Change License under the BSL 1.1 Change Date rules.

This is a **source-available / Business Source License** model before the Change Date, not OSI-approved Open Source during that period.

---

## 2. Recommended Base License

**Business Source License 1.1 (BSL 1.1)**

Use the unmodified BSL 1.1 license text supplied by MariaDB, filling only the license parameters and the Additional Use Grant.

The BSL model already provides:

- source availability;
- copying, modification, derivative-work creation, redistribution, and non-production use under its terms;
- an **Additional Use Grant** through which the licensor may permit limited production use;
- a separate commercial-license path for use outside the grant; and
- automatic conversion of each covered version to an Open Source Change License no later than the BSL-defined limit.

Do **not** rewrite the standard BSL 1.1 body. Project-specific production permissions should live in the **Additional Use Grant**.

---

## 3. Proposed BSL Parameter Block

The following is the proposed project-specific parameter block. Bracketed fields must be completed before adoption.

```text
Licensor:             [COPYRIGHT HOLDER / LEGAL ENTITY]

Licensed Work:        Cann-o-Call, version [VERSION]
                      Copyright © [YEAR] [COPYRIGHT HOLDER]

Additional Use Grant: See "Cann-o-Call Small-Business Production Use Grant"
                      below.

Change Date:          [YYYY-MM-DD]
                      Must comply with BSL 1.1's Change Date requirements for
                      this version.

Change License:       [SELECT GPL-COMPATIBLE OPEN SOURCE LICENSE]
                      Suggested candidates for legal review:
                      - Apache License 2.0, if confirmed appropriate for the
                        BSL 1.1 Change License covenant; or
                      - GNU GPL v2-or-later / GPL v3-or-later.
```

### Recommended Change-Date policy

For predictable versioning, consider setting each release's Change Date near the maximum period permitted by BSL 1.1, while recording the exact date in that release's license metadata. Do not use a vague phrase in the actual BSL parameter field if the adopted template requires a concrete date.

---

## 4. Draft Cann-o-Call Small-Business Production Use Grant

> **Draft — requires legal review before adoption**

### Additional Use Grant

In addition to the rights granted under the Business Source License 1.1, the Licensor grants permission to use the Licensed Work in **Production** without purchasing a separate commercial license when the Licensee qualifies as an **Eligible Small Organization** and the Production use is an **Eligible Internal Use**, as defined below.

### A. Eligible Small Organization

A Licensee is an **Eligible Small Organization** only if the Licensee and all of its Affiliates, considered together:

1. have **fewer than 50 full-time-equivalent employees**; **and**
2. had **less than USD $5,000,000 in gross revenue** during the most recently completed fiscal year.

For an organization that has not completed a fiscal year, gross revenue should be measured over the organization's operating period or another clearly specified trailing period in the final reviewed license.

If either threshold is met or exceeded, free Production use under this Additional Use Grant ends and continued Production use requires a separate commercial license from the Licensor.

### B. Eligible Internal Use

An **Eligible Internal Use** means Production use of the Licensed Work primarily for the Licensee's own internal operations, systems, development workflow, infrastructure, employees, or internal business processes.

Examples intended to qualify when the Licensee is an Eligible Small Organization include:

- internal automation;
- internal agent/runtime infrastructure;
- internal development tooling;
- internal data-processing or operational workflows; and
- software used by the Licensee's own personnel in carrying out the Licensee's business.

### C. Production Uses Not Granted Under This Additional Use Grant

No Production-use permission is granted by this Additional Use Grant when the Licensed Work is used to provide third parties with a commercial product or service whose principal value consists of Cann-o-Call itself or substantially equivalent Cann-o-Call functionality, including:

- hosting Cann-o-Call for third parties as a SaaS offering;
- selling or reselling access to Cann-o-Call;
- providing Cann-o-Call as a managed service;
- white-labeling Cann-o-Call for customers;
- redistributing a commercial offering whose principal product is the Licensed Work or a substantially equivalent derivative; or
- operating a service designed primarily to substitute for obtaining a commercial Cann-o-Call license.

Such Production use requires a separate commercial license regardless of employee count or revenue unless the Licensor grants separate written permission.

### D. Affiliates / Aggregation

For purposes of this Additional Use Grant, **Affiliate** should be defined in the final reviewed license to include entities that control, are controlled by, or are under common control with the Licensee.

Employee and revenue thresholds should be measured across the Licensee and its Affiliates collectively so that a large enterprise cannot qualify merely by placing use in a small subsidiary or special-purpose entity.

### E. Threshold Transition

A Licensee that ceases to qualify as an Eligible Small Organization should obtain a commercial license before continuing Production use beyond a reasonable transition period.

**Recommended review option:** include a **30-day transition period** after first exceeding a threshold solely to allow licensing arrangements to be completed. Counsel should confirm whether and how this grace period should be expressed.

### F. Commercial License

Production use not granted above requires a separate commercial license from the Licensor.

Commercial-license contact:

```text
[COMMERCIAL LICENSING EMAIL OR WEBSITE]
```

The commercial license may provide different rights, support terms, service rights, redistribution rights, enterprise deployment rights, or other negotiated permissions.

---

## 5. Threshold Policy — Proposed Default

| Measure | Free Small-Business Production Tier | Commercial License Required |
|---|---:|---:|
| Full-time-equivalent employees | **< 50** | **≥ 50** |
| Gross annual revenue | **< US $5,000,000** | **≥ US $5,000,000** |
| Internal production operations | Allowed if both thresholds satisfied | Commercial license if either threshold exceeded |
| Hosted/SaaS/resale/white-label service based principally on Cann-o-Call | Not granted | Commercial license required |
| Non-production evaluation/development | Governed by standard BSL 1.1 rights | Governed by standard BSL 1.1 rights |

### Why use both employee and revenue thresholds?

Using both limits avoids obvious edge cases:

- A five-person, very-high-revenue company does not receive the same free Production tier as a genuinely small operator.
- A large enterprise cannot qualify merely by assigning the software to a small team.
- Affiliate aggregation reduces subsidiary-based threshold avoidance.

The `50 FTE / US $5M` values are **policy defaults for review**, not immutable requirements. They can be changed before adoption.

---

## 6. Intended Examples

These examples explain policy intent and should not replace the final legal text.

| Example | Intended Result |
|---|---|
| Two-person business using Cann-o-Call for its own internal operations | Free Production use under Additional Use Grant |
| 20-person agency with US $2M annual revenue using it internally | Free Production use |
| 8-person startup with US $20M annual revenue | Commercial license required |
| 75-person company with US $3M annual revenue | Commercial license required |
| Large multinational/enterprise using it internally | Commercial license required |
| Small company selling hosted Cann-o-Call access to customers | Commercial license required |
| Developer evaluating or modifying the project outside Production | Standard BSL 1.1 rights apply |
| University researcher using it for non-production research | Standard BSL 1.1 rights apply; determine separately if Production-like institutional deployment occurs |

---

## 7. Copyright / Intellectual-Property Position

Suggested project notice:

```text
Copyright © [YEAR] [COPYRIGHT HOLDER]. All rights reserved except as expressly
licensed under the Business Source License 1.1 and any separately executed
commercial license.

Cann-o-Call is source-available under the Business Source License 1.1.
Qualified small organizations may receive limited Production-use rights under
the Cann-o-Call Small-Business Production Use Grant.

Enterprise, hosted-service, resale, white-label, and other Production uses not
covered by that grant require a separate commercial license.
```

This notice is explanatory. The actual rights should be controlled by the license documents, not the README summary.

---

## 8. Suggested Repository Files When Adopted

```text
LICENSE
  Unmodified BSL 1.1 text + completed parameter block / Additional Use Grant

LICENSE-COMMERCIAL.md
  Short explanation that commercial terms are available separately and how to
  contact the Licensor. Do not publish confidential negotiated pricing unless
  desired.

LICENSING.md
  Human-readable explanation of the small-business threshold, examples, FAQ,
  Change Date, and where the controlling license lives.

NOTICE
  Copyright/creator attribution and required notices.

README.md
  Short badge/text:
  "Source available under BSL 1.1. Free Production use for qualifying small
  organizations; commercial licensing available for enterprise/service use."
```

If accepting outside contributions, separately decide whether contributor terms (for example, a CLA or inbound-license policy) are needed to preserve the ability to dual-license future versions commercially.

---

## 9. Questions to Resolve Before Adoption

1. **Copyright holder:** Individual creator name, company/legal entity, or both?
2. **Employee cap:** Keep `<50 FTE`, or use another threshold?
3. **Revenue cap:** Keep `<US $5M`, or use another threshold?
4. **Revenue definition:** Gross revenue, annual recurring revenue, or another objective measure?
5. **Affiliate definition:** Exact control percentage / standard corporate-control definition?
6. **Grace period:** Immediate commercial-license requirement or 30-day transition after crossing a threshold?
7. **Service-provider treatment:** Should consultants/MSPs be allowed to operate Cann-o-Call solely on behalf of an otherwise eligible customer?
8. **Nonprofit/education production use:** Same thresholds, automatic free grant, or case-by-case permission?
9. **Change License:** Apache-2.0, GPL-2.0-or-later, GPL-3.0-or-later, or another BSL-compliant GPL-compatible license?
10. **Change Date cadence:** Maximum permitted period per version or a shorter period?
11. **Commercial contact:** Which public email/site should be listed?
12. **Contributor policy:** Whether external contributions require terms that preserve commercial relicensing rights.

---

## 10. Adoption Checklist

Before this becomes a real project license:

- [ ] Have the Additional Use Grant reviewed for BSL 1.1 compliance.
- [ ] Confirm the selected Change License satisfies BSL 1.1's covenant.
- [ ] Fill in Licensor, copyright holder, version, Change Date, and commercial contact.
- [ ] Confirm employee/revenue definitions are objective and administrable.
- [ ] Confirm Affiliate aggregation wording.
- [ ] Decide treatment of contractors, consultants, nonprofits, and educational institutions.
- [ ] Decide whether a threshold grace period exists.
- [ ] Confirm hosted-service/resale language matches the intended business model.
- [ ] Preserve the standard BSL 1.1 text without unauthorized modifications.
- [ ] Add a human-readable `LICENSING.md` summary that clearly states BSL is not OSI Open Source before the Change Date.
- [ ] Establish contributor/inbound-license policy before accepting significant third-party contributions if commercial dual licensing is important.
- [ ] Record the final licensing decision in the project build ledger / decisions documentation.

---

## 11. Source References for Review

The policy above is based on the official MariaDB BSL 1.1 materials current at the time of drafting:

- **Business Source License 1.1 — official license text:** https://mariadb.com/bsl11/
- **Adopting and Developing BSL Software — official FAQ:** https://mariadb.com/bsl-faq-adopting/
- **Projects using BSL 1.1 — official examples:** https://mariadb.com/projects-using-bsl-11/

Key points to independently verify before adoption:

- BSL 1.1 permits the Licensor to provide an Additional Use Grant for limited Production use.
- Use outside the rights currently granted may require a separate commercial license.
- BSL 1.1 is not an OSI Open Source license before conversion.
- Each covered version converts on the Change Date or under the BSL-defined maximum conversion period, whichever applies first.
- The Change License must satisfy BSL 1.1's GPL/GPL-compatibility covenant.

---

## Draft Disposition

```yaml
artifact: Cann-o-Call licensing policy draft
status: REVIEW_ONLY
base_license: Business Source License 1.1
model: source_available_then_open_source
free_production_tier:
  employee_threshold: "< 50 FTE"
  revenue_threshold: "< USD 5,000,000 gross annual revenue"
  threshold_logic: "both must be satisfied"
  use_scope: "eligible internal production use"
commercial_license_required:
  enterprise_threshold_exceeded: true
  hosted_service: true
  resale: true
  managed_service: true
  white_label: true
copyright_transferred: false
change_license: TBD
change_date: TBD
legal_review_required: true
```
