Correct, but let me walk through each step precisely:

1. **Deed lookup** → PBCPA shows the property is owned by `CLEMATIS VENTURES LLC` (an entity, not a person)

2. **Sunbiz lookup** → pipeline queries Florida's corporate registry for `CLEMATIS VENTURES LLC`, finds `Diament, Scott` listed as managing member at `333 Clematis Street, Suite 201`

3. **Fuzzy match** → `Diament, Scott` (from Sunbiz) is compared against the enriched skip-traced list. Slot 3 has `Scott Diament` — same person, name just formatted differently (Last, First vs First Last). Fuzzy match scores it 100 and confirms the match.

4. **Address selection** → Sunbiz member address (`333 Clematis Street, Suite 201`) is used since it's the most authoritative personal address available. It also happens to match the deed entity mailing address exactly, which is another corroborating signal — Scott is using his office address for both the LLC filing and the deed record.

So to directly answer your question: we're **not** finding his name on the deed — the deed only has the LLC name. We find his name by looking up who controls that LLC on Sunbiz, then confirming he was already in the skip-traced list. The enriched list match upgrades confidence to `high`.