defmodule Logflare.Source.BillingWriter do
  use GenServer

  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.BillingCounts
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

  def handle_info(:write_count, rls) do
    count = Data.get_node_inserts(rls.source.token)

    if count > 0 do
      BillingCounts.insert(rls.user, rls.source, %{
        node: Atom.to_string(Node.self()),
        count: 1
      })
      |> case do
        {:ok, _resp} ->
          :noop

        {:error, _resp} ->
          Logger.error("Error inserting billing count!", source_id: rls.source.token)
      end
    end

    write()
    {:noreply, rls}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{source_id: state.source_id})
    reason
  end

  defp write() do
    every = :timer.minutes(Enum.random(5..15))
    Process.send_after(self(), :write_count, every)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-bw")
  end
end
