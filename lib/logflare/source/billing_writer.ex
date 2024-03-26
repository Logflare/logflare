defmodule Logflare.Source.BillingWriter do
  @moduledoc false
  use GenServer

  alias Logflare.Billing.BillingCounts
  alias Logflare.Billing
  alias Logflare.Source.Data
  alias Logflare.Backends

  require Logger

  def start_link(args) do
    source = Keyword.get(args, :source)
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__))
  end

  def init(args) do
    source = Keyword.get(args, :source)
    write()
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       billing_last_node_count: 0,
       source: args[:source],
       source_token: source.token,
       plan: args[:plan],
       user: args[:user]
     }}
  end

  def handle_info(:write_count, state) do
    last_count = state.billing_last_node_count
    node_count = Data.get_node_inserts(state.source.token)
    count = node_count - last_count

    if count > 0 do
      record_to_db(state, count)

      if state.plan.type == "metered" do
        record_to_stripe(state, count)
      end
    end

    write()
    {:noreply, %{state | billing_last_node_count: node_count}}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{
      source_id: state.source_token
    })

    reason
  end

  defp write() do
    every = :timer.minutes(Enum.random(45..75))
    Process.send_after(self(), :write_count, every)
  end

  defp record_to_stripe(state, count) do
    billing_account = state.user.billing_account

    with %{"id" => si_id} <-
           Billing.get_billing_account_stripe_subscription_item(billing_account),
         {:ok, _response} <-
           Billing.Stripe.record_usage(si_id, count) do
      Logger.info("Successfully recorded usage counts (#{inspect(count)}) to Stripe",
        user_id: state.user.id,
        count: count
      )

      :noop
    else
      nil ->
        Logger.warning(
          "User's billing account does not have a stripe subscription item, ignoring usage record",
          user_id: state.user.id,
          count: count
        )

      {:error, resp} ->
        Logger.error("Error recording usage with Stripe. #{inspect(resp)}",
          source_id: state.source.token,
          error_string: inspect(resp)
        )
    end
  end

  defp record_to_db(state, count) do
    case BillingCounts.insert(state.user, state.source, %{
           node: Atom.to_string(Node.self()),
           count: count
         }) do
      {:ok, _resp} ->
        :noop

      {:error, _resp} ->
        Logger.error("Error inserting billing count!",
          source_id: state.source.token
        )
    end
  end
end
