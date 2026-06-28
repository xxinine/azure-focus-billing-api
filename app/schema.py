"""Canonical FOCUS schema (v1.2 / v1.3) and normalization helpers.

Source of truth (verified against official docs, not guessed):
- FOCUS Column Library v1.2 -> 57 columns
  https://focus.finops.org/focus-columns/?version=v1-2
- FOCUS Column Library v1.3 -> 65 columns
  https://focus.finops.org/focus-columns/?version=v1-3&dataset=cost-and-usage

v1.3 == v1.2 + 8 new columns (no renames, no removals):
  Allocation (new category):
    AllocatedMethodDetails, AllocatedMethodId, AllocatedResourceId,
    AllocatedResourceName, AllocatedTags
  Charge Origination (added):
    HostProviderName, ServiceProviderName
  Contract (new category):
    ContractApplied

Real Azure export reality (dataVersion="1.2-preview"):
- Provides 53 of the 57 FOCUS v1.2 standard columns. The 4 it omits as
  standard columns are: AvailabilityZone, PricingCurrencyContractedUnitPrice,
  PricingCurrencyEffectiveCost, PricingCurrencyListUnitPrice.
- Adds 54 Azure-specific "x_" extension columns (e.g. x_BilledCostInUsd,
  x_BillingExchangeRate, x_ResourceGroupName).

Normalization fills any missing canonical column with NULL (no value guessing)
and passes through all x_ extension columns unchanged. When Azure ships native
1.3, the 8 new columns simply pass through.
"""
from __future__ import annotations

# --- FOCUS v1.2 standard columns (57), per official Column Library ---
FOCUS_1_2_COLUMNS: list[str] = [
    # Account (6)
    "BillingAccountId",
    "BillingAccountName",
    "BillingAccountType",
    "SubAccountId",
    "SubAccountName",
    "SubAccountType",
    # Billing (9)
    "BilledCost",
    "BillingCurrency",
    "ConsumedQuantity",
    "ConsumedUnit",
    "ContractedCost",
    "ContractedUnitPrice",
    "EffectiveCost",
    "ListCost",
    "ListUnitPrice",
    # Capacity Reservation (2)
    "CapacityReservationId",
    "CapacityReservationStatus",
    # Charge (4)
    "ChargeCategory",
    "ChargeClass",
    "ChargeDescription",
    "ChargeFrequency",
    # Charge Origination (4)
    "InvoiceId",
    "InvoiceIssuerName",
    "ProviderName",
    "PublisherName",
    # Commitment Discount (7)
    "CommitmentDiscountCategory",
    "CommitmentDiscountId",
    "CommitmentDiscountName",
    "CommitmentDiscountQuantity",
    "CommitmentDiscountStatus",
    "CommitmentDiscountType",
    "CommitmentDiscountUnit",
    # Location (3)
    "AvailabilityZone",
    "RegionId",
    "RegionName",
    # Pricing (7)
    "PricingCategory",
    "PricingCurrency",
    "PricingCurrencyContractedUnitPrice",
    "PricingCurrencyEffectiveCost",
    "PricingCurrencyListUnitPrice",
    "PricingQuantity",
    "PricingUnit",
    # Resource (4)
    "ResourceId",
    "ResourceName",
    "ResourceType",
    "Tags",
    # Service (3)
    "ServiceCategory",
    "ServiceName",
    "ServiceSubcategory",
    # SKU (4)
    "SkuId",
    "SkuMeter",
    "SkuPriceDetails",
    "SkuPriceId",
    # Timeframe (4)
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "ChargePeriodEnd",
    "ChargePeriodStart",
]

# --- FOCUS v1.3 additions (8), per official Column Library ---
FOCUS_1_3_ADDED_COLUMNS: list[str] = [
    # Allocation (new category)
    "AllocatedMethodDetails",
    "AllocatedMethodId",
    "AllocatedResourceId",
    "AllocatedResourceName",
    "AllocatedTags",
    # Charge Origination (added)
    "HostProviderName",
    "ServiceProviderName",
    # Contract (new category)
    "ContractApplied",
]

# Canonical target = FOCUS v1.3 (65 columns)
FOCUS_1_3_COLUMNS: list[str] = FOCUS_1_2_COLUMNS + FOCUS_1_3_ADDED_COLUMNS

# Columns the real Azure 1.2-preview export omits as standard (filled as NULL).
AZURE_12_PREVIEW_MISSING: list[str] = [
    "AvailabilityZone",
    "PricingCurrencyContractedUnitPrice",
    "PricingCurrencyEffectiveCost",
    "PricingCurrencyListUnitPrice",
]

# Explicit rename map (v1.2 -> v1.3). Empty: official diff has no renames.
RENAME_MAP_V12_TO_V13: dict[str, str] = {}

# Timestamp columns used for date filtering / partition derivation.
CHARGE_PERIOD_START = "ChargePeriodStart"
CHARGE_PERIOD_END = "ChargePeriodEnd"
BILLING_PERIOD_START = "BillingPeriodStart"


def build_normalization_select(
    source_columns: list[str], include_extension_columns: bool = True
) -> str:
    """Project source columns onto canonical FOCUS 1.3, optionally keeping x_.

    For each canonical column:
      - present (after rename map) -> passthrough
      - missing                    -> NULL AS <col>
    Extension columns (x_*) present in the source are appended unchanged.
    """
    available = set(source_columns)
    reverse_rename = {v: k for k, v in RENAME_MAP_V12_TO_V13.items()}

    projections: list[str] = []
    for col in FOCUS_1_3_COLUMNS:
        if col in available:
            projections.append(f'"{col}" AS "{col}"')
        elif col in reverse_rename and reverse_rename[col] in available:
            projections.append(f'"{reverse_rename[col]}" AS "{col}"')
        else:
            projections.append(f'NULL AS "{col}"')

    if include_extension_columns:
        canonical = set(FOCUS_1_3_COLUMNS)
        for col in source_columns:
            if col.startswith("x_") and col not in canonical:
                projections.append(f'"{col}" AS "{col}"')

    return ",\n  ".join(projections)
