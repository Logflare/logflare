defmodule Logflare.Source.BillingWriterTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Billing.BillingCount
  alias Logflare.Repo
  alias Logflare.Source.BillingWriter
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources.Counters
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    start_supervised!(AllLogsLogged)

    user = insert(:user)
    source = insert(:source, user: user)
    _billing_account = insert(:billing_account, user: user)
    user = user |> Logflare.Repo.preload(:billing_account)
    plan = insert(:plan, type: "metered")

    pid = start_supervised!({BillingWriter, source: source, user: user, plan: plan})

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
