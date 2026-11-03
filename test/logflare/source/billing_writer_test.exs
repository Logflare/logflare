defmodule Logflare.Sources.Source.BillingWriterTest do
  @moduledoc false
  use Logflare.DataCase

  import ExUnit.CaptureLog
  alias Ecto.Adapters.SQL
  alias Logflare.Repo
  alias Logflare.Sources.Source.BillingWriter
  alias Logflare.Sources.Counters
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    start_supervised!(AllLogsLogged)

    user = insert(:user)
    source = insert(:source, user: user)
    plan = insert(:plan, type: "metered", name: "Metered")
    insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)

    pid = start_supervised!({BillingWriter, source: source})

    {:ok, pid: pid, source: source}
  end

  test ":write_count records usage with Stripe only", %{pid: pid, source: source} do
    expect_usage_recorded()
    Counters.increment(source.token)
    send(pid, :write_count)

    assert_receive :usage_recorded, 2_000
    assert billing_counts_rows() == 0
  end

  test "logs that the database is unavailable rather than a missing subscription item",
       %{pid: pid, source: source} do
    Logflare.Billing.Cache
    |> stub(:get_billing_account_by, fn _kv -> {:error, :database_unavailable} end)

    Counters.increment(source.token)

    log =
      capture_log(fn ->
        send(pid, :write_count)
        refute_receive :usage_recorded, 500
      end)

    assert log =~ "database unavailable"
    refute log =~ "does not have a stripe subscription item"
  end

  test "keeps the count when usage could not be recorded, so the next tick retries it",
       %{pid: pid, source: source} do
    Logflare.Billing.Cache
    |> stub(:get_billing_account_by, fn _kv -> {:error, :database_unavailable} end)

    Counters.increment(source.token)
    before = :sys.get_state(pid).billing_last_node_count

    capture_log(fn -> send(pid, :write_count) end)
    # a synchronous call flushes the cast above
    assert :sys.get_state(pid).billing_last_node_count == before
  end

  test "advances the count when the account has no subscription item",
       %{pid: pid, source: source} do
    Logflare.Billing
    |> stub(:get_billing_account_stripe_subscription_item, fn _account -> nil end)

    Counters.increment(source.token)

    capture_log(fn -> send(pid, :write_count) end)
    assert :sys.get_state(pid).billing_last_node_count > 0
  end

  defp expect_usage_recorded(test_pid \\ self()) do
    Stripe.UsageRecord
    |> expect(:create, fn sub_item_id, _params ->
      if is_nil(sub_item_id) do
        raise "subscription item id should not be nil"
      end

      send(test_pid, :usage_recorded)
      {:ok, %{}}
    end)
  end

  defp billing_counts_rows do
    %{rows: [[count]]} = SQL.query!(Repo, "select count(*) from billing_counts")
    count
  end
end
