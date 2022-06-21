defmodule Logflare.Source.BillingWriterTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.{BillingWriter, RecentLogsServer}
  alias Logflare.Billing.BillingCount
  alias Logflare.Repo
  setup :set_mimic_global

  setup do
    user = insert(:user)
    source = insert(:source, user: user)
    _billing_account = insert(:billing_account, user: user)
    user = user |> Logflare.Repo.preload(:billing_account)
    plan = insert(:plan, type: "metered")

    pid =
      start_supervised!(
        {BillingWriter,
         %RecentLogsServer{source_id: source.token, source: source, user: user, plan: plan}}
      )

    # increase log count
    start_supervised!(Logflare.Sources.Counters)
    Logflare.Sources.Counters.incriment(source.token)

    # Stripe mocks
    Stripe.SubscriptionItem.Usage
    |> expect(:create, fn sub_item_id, _params ->
      if is_nil(sub_item_id) do
        raise "subscription item id should not be nil"
      end

      {:ok, %{}}
    end)

    {:ok, pid: pid}
  end

  test ":write_count", %{pid: pid} do
    send(pid, :write_count)
    :timer.sleep(200)
    assert Repo.aggregate(BillingCount, :count) == 1
  end
end
