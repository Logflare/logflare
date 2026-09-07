defmodule Logflare.Sources.Source.BillingWriterTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Billing.BillingCount
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

    test_pid = self()

    Stripe.UsageRecord
    |> expect(:create, fn sub_item_id, _params ->
      if is_nil(sub_item_id) do
        raise "subscription item id should not be nil"
      end

      send(test_pid, :usage_recorded)
      {:ok, %{}}
    end)

    {:ok, pid: pid, source: source}
  end

  test ":write_count records usage with Stripe only", %{pid: pid, source: source} do
    Counters.increment(source.token)
    send(pid, :write_count)

    assert_receive :usage_recorded, 2_000
    assert Repo.aggregate(BillingCount, :count) == 0
  end
end
