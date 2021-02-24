defmodule Logflare.Repo.MaxPartitionedRowsWorker do
  use GenServer
  use Logflare.Commons
  @interval_ms 5_000

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  def start_link(args, opts \\ []) when is_list(args) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    schedule_action()
    {:ok, %{schemas_and_opts: args}}
  end

  @impl true
  def handle_info(:action, state) do
    for %{schema: schema, opts: opts} <- state.schemas_and_opts do
      Repo.TableManagement.delete_all_rows_over_limit_with_opts(schema, opts) |> IO.inspect()
    end

    {:noreply, state}
  end

  defp schedule_action() do
    Process.send_after(self(), :action, @interval_ms)
  end
end
