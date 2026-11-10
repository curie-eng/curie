"""Keep ADR 0237's explicit reproduction command as a collected-suite alias."""

import test_hook_receipt_retention as retained

hook_agent = retained.hook_agent
test_faithful_retry_after_settled_retention_retains_receipt = (
    retained.test_faithful_retry_after_settled_retention_retains_receipt
)
