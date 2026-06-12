# Data Quality Report
**Run Timestamp:** 2026-06-12T00:20:23.837267

---

## Extraction Summary
| Metric | Count |
|--------|------:|
| Users extracted | 1,100 |
| Products extracted | 1,100 |
| Weblogs extracted | 15,000 |
| **Total rows extracted** | **17,200** |

## Validation & Cleaning
| Issue | Count |
|-------|------:|
| Duplicate rows removed | 468 |
| Invalid timestamps | 675 |
| Null user IDs | 763 |
| Null/malformed session IDs | 676 |
| Orphan user IDs (in logs, not in users) | 1,196 |
| Orphan product IDs (in logs, not in products) | 1,235 |
| **Total invalid/skipped rows** | **2,582** |
| **Rows after cleaning** | **10,038** |

## Transformation Summary
| Metric | Count |
|--------|------:|
| Sessions computed | 4,304 |
| Abandoned cart sessions | 1,229 |
| High-activity sessions (>50 actions) | 0 |
| Negative duration sessions | 0 |

## Load Summary
| Table | Rows Loaded |
|-------|------------:|
| dim_user | 1,069 |
| dim_product | 1,080 |
| fact_user_activity | 10,038 |
| agg_session_metrics | 4,304 |
| **Total rows loaded** | **16,491** |

## Anomalies
No anomalies detected.
