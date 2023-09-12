defmodule Logflare.Source.BillingWriter do
  @moduledoc false
  use GenServer

  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Billing.BillingCounts
  alias Logflare.Billing
  alias Logflare.Source.Data
  alias Logflare.Source

  require Logger

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: Source.Supervisor.start_via(__MODULE__, source_id))
  end

  def init(rls) do
    write()
    Process.flag(:trap_exit, true)

    {:ok, rls}
  end

  def handle_info(:write_count, rls) do
    last_count = rls.billing_last_node_count
    node_count = Data.get_node_inserts(rls.source.token)
    count = node_count - last_count

    if count > 0 do
      record_to_db(rls, count)

      if rls.plan.type == "metered" do
        record_to_stripe(rls, count)
      end
    end

    write()
    {:noreply, %{rls | billing_last_node_count: node_count}}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{source_id: state.source_id})
    reason
  end

  defp write() do
    every = :timer.minutes(Enum.random(45..75))
    Process.send_after(self(), :write_count, every)
  end

  defp record_to_stripe(rls, count) do
    billing_account = rls.user.billing_account

    with %{"id" => si_id} <-
           Billing.get_billing_account_stripe_subscription_item(billing_account),
         {:ok, _response} <-
           Billing.Stripe.record_usage(si_id, count) do
      Logger.info("Successfully recorded usage counts (#{inspect(count)}) to Stripe",
        user_id: rls.user.id,
        count: count
      )

      :noop
    else
      nil ->
        Logger.warning(
          "User's billing account does not have a stripe subscription item, ignoring usage record",
          user_id: rls.user.id,
          count: count
        )

      {:error, resp} ->
        Logger.error("Error recording usage with Stripe. #{inspect(resp)}",
          source_id: rls.source.token,
          error_string: inspect(resp)
        )
    end
  end

  defp record_to_db(rls, count) do
    with {:ok, _resp} <-
           BillingCounts.insert(rls.user, rls.source, %{
             node: Atom.to_string(Node.self()),
             count: count
           }) do
      :noop
    else
      {:error, _resp} ->
        Logger.error("Error inserting billing count!",
          source_id: rls.source.token
        )
    end
  end
end
