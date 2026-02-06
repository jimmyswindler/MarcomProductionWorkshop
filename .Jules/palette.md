## 2025-02-12 - Scanning Feedback
**Learning:** In scanning applications, visual feedback (status messages) is often missed by screen reader users because focus remains on the input field.
**Action:** Always add `aria-live="polite"` (or `assertive` for errors) to status message containers so updates are announced automatically.
