defmodule Logflare.Sources.Source.BillingWriter do
  @moduledoc false
  use GenServer

  alias Logflare.Billing.BillingCounts
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
    user = Users.Cache.get(source.user_id)
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
    last_count = state.billing_last_node_count
    node_count = Data.get_node_inserts(state.source.token)
    count = node_count - last_count

    if count > 0 do
      record_to_db(state, count)

      if state.plan_type == "metered" do
        record_to_stripe(state, count)
      end
    end

    write()
    {:noreply, %{state | billing_last_node_count: node_count}}
  end

  defp write do
    every = :timer.minutes(Enum.random(45..75))
    Process.send_after(self(), :write_count, every)
  end

  defp record_to_stripe(state, count) do
    billing_account = Billing.Cache.get_billing_account_by(user_id: state.user_id)

    with %{"id" => si_id} <-
           Billing.get_billing_account_stripe_subscription_item(billing_account),
         {:ok, _response} <-
           Billing.Stripe.record_usage(si_id, count) do
      :noop
    else
      nil ->
        Logger.warning(
          "User's billing account does not have a stripe subscription item, ignoring usage record",
          user_id: state.user_id,
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
    user = Users.Cache.get(state.user_id)

    case BillingCounts.insert(user, state.source, %{
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
