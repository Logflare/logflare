defmodule Logflare.AlertIssues.SignalsTest do
  use ExUnit.Case, async: true

  alias Logflare.AlertIssues.Signals
  alias Logflare.Alerting.AlertQuery

  defp alert(attrs \\ %{}) do
    struct!(
      %AlertQuery{name: "Edge function errors", rule_key: "logflare.edge-fn-5xx", group_by: []},
      attrs
    )
  end

  test "rows without group_by become one firing signal" do
    rows = [%{"count" => 3}, %{"count" => 4}]

    assert [%{status: :firing, title: "Edge function errors", metadata: metadata} = signal] =
             Signals.from_rows(alert(), rows)

    assert signal.fingerprint == Signals.fingerprint("logflare.edge-fn-5xx", %{})
    assert metadata["sample_rows"] == rows
    assert metadata["row_count"] == 2
  end

  test "rows are grouped by group_by into one signal per group" do
    rows = [
      %{"project" => "a", "function_id" => "f1", "count" => 1},
      %{"project" => "a", "function_id" => "f2", "count" => 2},
      %{"project" => "a", "function_id" => "f1", "count" => 3}
    ]

    signals = Signals.from_rows(alert(%{group_by: ["project", "function_id"]}), rows)

    assert length(signals) == 2
    f1 = Enum.find(signals, &(&1.identifiers["function_id"] == "f1"))
    assert f1.identifiers == %{"project" => "a", "function_id" => "f1"}
    assert f1.metadata["row_count"] == 2
    assert f1.title == "Edge function errors: a, f1"
  end

  test "fingerprints are stable regardless of key order" do
    assert Signals.fingerprint("k", %{"a" => "1", "b" => "2"}) ==
             Signals.fingerprint("k", %{"b" => "2", "a" => "1"})

    refute Signals.fingerprint("k", %{"a" => "1"}) == Signals.fingerprint("k", %{"a" => "2"})
    refute Signals.fingerprint("k1", %{}) == Signals.fingerprint("k2", %{})
  end

  test "signal_mapping identifiers copy row columns and scope adds literal identifiers" do
    alert =
      alert(%{
        group_by: ["function_id"],
        signal_mapping: %{"identifiers" => %{"function" => "function_id", "ref" => "project"}},
        scope: %{"project" => "default"}
      })

    [signal] = Signals.from_rows(alert, [%{"function_id" => "f1", "project" => "p1"}])

    assert signal.identifiers == %{
             "function_id" => "f1",
             "function" => "f1",
             "ref" => "p1",
             "project" => "default"
           }
  end

  test "sample rows are capped at 5" do
    rows = for n <- 1..8, do: %{"n" => n}
    [signal] = Signals.from_rows(alert(), rows)
    assert length(signal.metadata["sample_rows"]) == 5
    assert signal.metadata["row_count"] == 8
  end

  test "group values are stringified for identifiers" do
    [signal] =
      Signals.from_rows(alert(%{group_by: ["status"]}), [%{"status" => 500, "n" => 1}])

    assert signal.identifiers == %{"status" => "500"}
  end

  test "signal_mapping columns set the title, description, severity, category and metadata" do
    alert =
      alert(%{
        group_by: ["lint_name", "entity"],
        signal_mapping: %{
          "title_column" => "title",
          "description_column" => "detail",
          "severity_column" => "severity",
          "category_column" => "category",
          "metadata_columns" => ["remediation", "lint_name"]
        }
      })

    row = %{
      "lint_name" => "rls_disabled_in_public",
      "entity" => "public.orders",
      "title" => "RLS Disabled in Public: public.orders",
      "detail" => "Table public.orders is public, but RLS has not been enabled.",
      "severity" => "critical",
      "category" => "security",
      "remediation" => "https://supabase.com/docs/guides/database/database-linter?lint=0013"
    }

    [signal] = Signals.from_rows(alert, [row])

    assert signal.title == "RLS Disabled in Public: public.orders"
    assert signal.description == row["detail"]
    assert signal.severity == :critical
    assert signal.category == :security
    assert signal.metadata["remediation"] == row["remediation"]
    assert signal.metadata["lint_name"] == "rls_disabled_in_public"
    assert signal.metadata["row_count"] == 1
  end

  test "invalid or missing mapped values fall back to the alert defaults" do
    alert =
      alert(%{
        signal_mapping: %{
          "title_column" => "title",
          "severity_column" => "severity",
          "category_column" => "category"
        }
      })

    [signal] = Signals.from_rows(alert, [%{"severity" => "ERROR", "category" => "nope"}])

    assert signal.title == "Edge function errors"
    refute Map.has_key?(signal, :severity)
    refute Map.has_key?(signal, :category)
    refute Map.has_key?(signal, :description)
  end
end
