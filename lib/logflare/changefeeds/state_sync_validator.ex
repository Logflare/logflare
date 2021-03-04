defmodule Logflare.Changefeeds.RepoStateSyncValidator do
  require Logger
  use Logflare.Commons
  use GenServer
  alias Changefeeds.ChangefeedSubscription
  import Ecto.Query

  @opts_definition [
    interval_sec: [
      type: :non_neg_integer,
      default: 5
    ]
  ]

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    {:ok, opts} = NimbleOptions.validate(args, @opts_definition)

    {:ok, Map.new(opts), {:continue, :after_init}}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  @impl true
  def handle_continue(:after_init, state) do
    run_and_schedule(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:work, state) do
    run_and_schedule(state)
    {:noreply, state}
  end

  defp run_and_schedule(%{interval_sec: interval_sec} = state) do
    validate_changefeed_tables(state)
    Process.send_after(self(), :work, :timer.seconds(interval_sec))
  end

  def validate_changefeed_tables(state) do
    for %ChangefeedSubscription{schema: schema} <- Changefeeds.list_changefeed_subscriptions() do
      validate(schema, state)
    end
  end

  @spec validate(schema :: module(), map()) :: :ok | {:error, list}
  def validate(schema, %{interval_sec: interval_sec}) do
    q =
      schema
      |> from()
      |> where([t], t.updated_at >= ago(^interval_sec, "second"))
      |> order_by([t], desc: t.updated_at)

    global_data = Repo.all(q)
    local_data = LocalRepo.all(q)

    if global_data == local_data do
      :ok
    else
      table = EctoSchemaReflection.source(schema)

      diff =
        MapSet.new(global_data)
        |> MapSet.difference(MapSet.new(local_data))
        |> MapSet.to_list()

      Logger.warn(
        "Node #{Node.self()} global and local repo state difference detected for table #{table}: #{
          inspect(diff)
        }"
      )

      {:error, diff}
    end
  end
end
