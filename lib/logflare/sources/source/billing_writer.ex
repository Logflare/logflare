defmodule Logflare.Sources.Source.BillingWriter do
  @moduledoc false
  use GenServer

  alias Logflare.Billing
  alias Logflare.Sources.Source.Data
  alias Logflare.Backends
  alias Logflare.Users

  require Logger

  def start_link(args) do
    source = Keyword.get(args, :source)
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__))
  end

  def init(args) do
    source = Keyword.get(args, :source)
    write()
    user = Users.Cache.get!(source.user_id)
    plan = Billing.Cache.get_plan_by_user(user)

    {:ok,
     %{
       billing_last_node_count: 0,
       source: args[:source],
       source_token: source.token,
       plan_type: plan.type,
       user_id: source.user_id
     }}
  end

  def handle_info(:write_count, state) do
    node_count = Data.get_node_inserts(state.source.token)
    count = node_count - state.billing_last_node_count

    write()

    {:noreply, record_usage(state, node_count, count)}
  end

  defp record_usage(%{plan_type: "metered"} = state, node_count, count) when count > 0 do
    case record_to_stripe(state, count) do
      :ok ->
        advance(state, node_count)

      {:error, :no_subscription_item} ->
        advance(state, node_count)

      {:error, _transient} ->
        state
    end
  end

  defp record_usage(state, node_count, _count), do: advance(state, node_count)

  defp advance(state, node_count), do: %{state | billing_last_node_count: node_count}

  defp write do
    every = :timer.minutes(Enum.random(45..75))
    Process.send_after(self(), :write_count, every)
  end

  defp record_to_stripe(state, count) do
    case Billing.Cache.get_billing_account_by(user_id: state.user_id) do
      {:error, :database_unavailable} = err ->
        Logger.warning("Could not load billing account to record usage, database unavailable",
          user_id: state.user_id,
          count: count
        )

        err

      billing_account ->
        do_record_to_stripe(state, billing_account, count)
    end
  end

  defp do_record_to_stripe(state, billing_account, count) do
    with %{"id" => si_id} <-
           Billing.get_billing_account_stripe_subscription_item(billing_account),
         {:ok, _response} <-
           Billing.Stripe.record_usage(si_id, count) do
      :ok
    else
      nil ->
        Logger.warning(
          "User's billing account does not have a stripe subscription item, ignoring usage record",
          user_id: state.user_id,
          count: count
        )

        {:error, :no_subscription_item}

      {:error, resp} = err ->
        Logger.error("Error recording usage with Stripe. #{inspect(resp)}",
          source_id: state.source.token,
          error_string: inspect(resp)
        )

        err
    end
  end
end
