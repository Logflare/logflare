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
end
