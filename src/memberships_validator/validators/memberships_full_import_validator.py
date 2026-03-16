from __future__ import annotations

from memberships_validator.validators.memberships_daily_maintenance_validator import MembershipsDailyMaintenanceValidator
from memberships_validator.config.memberships_full_import_config import MODE_NAME, DATASET_NAME


class MembershipsFullImportValidator(MembershipsDailyMaintenanceValidator):
    def add_check(
        self,
        check_id: str,
        check_name: str,
        status: str,
        category: str,
        country: str = "ALL",
        field: str = "",
        expected: str = "",
        actual: str = "",
        details: str = "",
        evidence_file: str = "",
    ) -> None:
        from prices_validator.core.types import CheckResult

        self.checks.append(
            CheckResult(
                check_id=check_id.replace("MEM-DAILY", "MEM-FULL"),
                check_name=check_name.replace("Daily Maintenance", "Full Import"),
                status=status,
                category=category,
                dataset=f"{DATASET_NAME} - {MODE_NAME}",
                country=country,
                field=field,
                expected=expected,
                actual=actual,
                details=details,
                evidence_file=evidence_file,
            )
        )