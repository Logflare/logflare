defmodule Logflare.Source.BillingWriter do
  use GenServer
  use Logflare.Commons

  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.BillingCounts
  alias Logflare.BillingAccounts
  alias Logflare.Source.Data

  require Logger

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: name(source_id))
  end

  def init(rls) do
    write()
    Process.flag(:trap_exit, true)

    {:ok, rls}
  end

  @spec handle_info(:write_count, RLS.t()) :: {:noreply, term}
  def handle_info(:write_count, %RLS{} = rls) do
    last_count = rls.billing_last_node_count
    source = Sources.get_source!(rls.source_id)
    plan = Plans.get_plan!(rls.plan_id)
    node_count = Data.get_node_inserts(source.token)
    count = node_count - last_count

    if count > 0 do
      record_to_db(rls, count)

      if plan.type == "metered" do
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

  @spec name(atom()) :: atom()
  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-bw")
  end

  @spec record_to_stripe(RLS.t(), integer) :: :noop | :ok
  defp record_to_stripe(%RLS{} = rls, count) when is_integer(count) do
    user = Users.get_user!(rls.user_id)
    billing_account = user.billing_account

    with {:ok, si} <-
           BillingAccounts.get_billing_account_stripe_subscription_item(billing_account),
         {:ok, _response} <-
           BillingAccounts.Stripe.record_usage(si["id"], count) do
      :noop
    else
      {:error, resp} ->
        Logger.error("Error recording usage with Stripe",
          source_id: rls.source_id,
          error_string: inspect(resp)
        )
    end
  end

  @spec record_to_db(RLS.t(), integer) :: :noop | :ok
  defp record_to_db(%RLS{} = rls, count) when is_integer(count) do
    source = Sources.get_source(rls.source_id)
    user = Users.get_user(rls.user_id)

    with {:ok, _resp} <-
           BillingCounts.insert(user, source, %{
             node: Atom.to_string(Node.self()),
             count: count
           }) do
      :noop
    else
      {:error, _resp} ->
        Logger.error("Error inserting billing count!",
          source_id: rls.source_id
        )
    end
  end
end
