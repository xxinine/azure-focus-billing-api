from types import SimpleNamespace

from ingestion import ingest


class _DescriptionResult:
    def fetchall(self):
        return [("BilledCost",)]


class _Connection:
    def execute(self, _query, _params):
        return _DescriptionResult()


def test_ingest_logs_billing_and_tax_basis_stages(monkeypatch, capsys):
    settings = SimpleNamespace()
    subscription = SimpleNamespace(subscription_key="china", cloud="china")
    write_order = []
    timestamp = "2026-09-08T07:20:31+00:00"

    monkeypatch.setattr(ingest, "get_settings", lambda: settings)
    monkeypatch.setattr(ingest, "_raw_glob", lambda *_args: "raw/*.parquet")
    monkeypatch.setattr(ingest, "build_normalization_select", lambda _cols: '"BilledCost"')
    monkeypatch.setattr(ingest, "_log", lambda message: print(f"{timestamp} {message}"))

    def write_billing(*_args, **_kwargs):
        write_order.append("billing")
        return "az://focus/curated/focus/data.parquet", 4189

    def write_tax_basis(*_args, **_kwargs):
        write_order.append("tax basis")
        return "az://focus/curated/tax-basis/data.parquet", 120

    monkeypatch.setattr(ingest, "write_partition", write_billing)
    monkeypatch.setattr(ingest, "write_tax_basis_partition", write_tax_basis)

    result = ingest.ingest_partition(_Connection(), subscription, "daily", "2026-09")

    assert write_order == ["billing", "tax basis"]
    assert result["taxBasisRows"] == 120
    assert capsys.readouterr().out.splitlines() == [
        f"{timestamp} [start] billing china daily 2026-09",
        f"{timestamp} [ok] billing china daily 2026-09: 4189 rows -> az://focus/curated/focus/data.parquet",
        f"{timestamp} [start] tax basis china daily 2026-09",
        f"{timestamp} [ok] tax basis china daily 2026-09: 120 rows -> az://focus/curated/tax-basis/data.parquet",
    ]