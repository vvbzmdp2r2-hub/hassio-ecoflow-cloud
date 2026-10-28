# 1.7.3rc1 — prepared candidate

Based on TelosNox v1.7.2.2 and fix 58a79d5b8faab5c3c4b61e667f51f0cfac754b03.

Accepts serial-matched STREAM 5000 peer SOC/power from a known live master in the same MQTT client. Retained, unknown and ambiguous duplicate sources are rejected. Missing fields are not zero-filled. Commands and unaddressed system/configuration data keep their own-device path.

Domain, unique IDs, device identifiers, config-entry migration, runtime dependencies and control policies are unchanged. Apache-2.0 and fixture MIT notices retained.

Underlying fix: GitHub stable/beta jobs passed 147 tests each; lint, hassfest and all 10 HACS checks passed. Candidate metadata requires final CI before publication. No tag, release, live installation or physical validation yet.
