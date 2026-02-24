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

    # Stripe mocks
    Stripe.SubscriptionItem.Usage
    |> expect(:create, fn sub_item_id, _params ->
      if is_nil(sub_item_id) do
        raise "subscription item id should not be nil"
      end

      {:ok, %{}}
    end)

    {:ok, pid: pid, source: source}
  end

  test ":write_count", %{pid: pid, source: source} do
    # increase log count
    Counters.increment(source.token)
    send(pid, :write_count)
    :timer.sleep(200)
    assert Repo.aggregate(BillingCount, :count) == 1
  end
end
