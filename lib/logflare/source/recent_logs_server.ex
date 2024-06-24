defmodule Logflare.Source.RecentLogsServer do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """
  use TypedStruct
  use GenServer

  alias Logflare.TaskSupervisor
  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources
  alias Logflare.PubSubRates
  alias Logflare.Cluster
  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Source

  require Logger

  @touch_timer :timer.minutes(45)
  @broadcast_every 1_500

  @spec push(Source.t(), LE.t() | [LE.t()]) :: :ok
  def push(source, %LE{} = le), do: push(source, [le])
  def push(_source, []), do: :ok

  def push(%Source{token: source_token}, log_events) when is_list(log_events) do
    case Backends.lookup(__MODULE__, source_token) do
      {:ok, pid} -> GenServer.cast(pid, {:push, source_token, log_events})
      {:error, _} -> :ok
    end
  end

  @spec list(Source.t() | atom()) :: [LE.t()]
  def list(%Source{token: token}), do: list(token)

  def list(source_token) when is_atom(source_token) do
    case Backends.lookup(__MODULE__, source_token) do
      {:ok, pid} ->
        {:ok, logs} = GenServer.call(pid, :list)
        logs

      {:error, _} ->
        []
    end
  end

  def list_for_cluster(%Source{token: token}), do: list_for_cluster(token)

  def list_for_cluster(source_token) when is_atom(source_token) do
    nodes = Cluster.Utils.node_list_all()

    task =
      Task.async(fn ->
        nodes
        |> Enum.map(
          &Task.Supervisor.async({TaskSupervisor, &1}, __MODULE__, :list, [source_token])
        )
        |> Task.yield_many()
        |> Enum.map(fn {%Task{pid: pid}, res} ->
          res || Task.Supervisor.terminate_child(TaskSupervisor, pid)
        end)
      end)

    case Task.yield(task, 5_000) || Task.shutdown(task) do
      {:ok, results} ->
        results
        |> Enum.map(fn {:ok, events} -> events end)
        |> List.flatten()
        |> Enum.sort_by(& &1.body["timestamp"], &<=/2)
        |> Enum.take(-100)

      _else ->
        list(source_token)
    end
  end

  def get_latest_date(source_token) when is_atom(source_token) do
    case Backends.lookup(__MODULE__, source_token) do
      {:ok, pid} ->
        case GenServer.call(pid, :latest_le) do
          {:ok, log_event} -> log_event.body["timestamp"]
          {:error, _reason} -> 0
        end

      {:error, _} ->
        0
    end
  end

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: Backends.via_source(args[:source], __MODULE__),
      spawn_opt: [
        fullsweep_after: 20
      ],
      hibernate_after: 5_000
    )
  end

  ## Client
  def init(args) do
    source = Keyword.get(args, :source)

    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: source.token, source_token: source.token)

    touch()
    broadcast()
    load_init_log_message(source.token)

    Logger.info("[#{__MODULE__}] Started")

    {:ok,
     %{
       inserts_since_boot: 0,
       total_cluster_inserts: 0,
       source_token: source.token,
       latest_log_event: nil,
       recent: LQueue.new(100)
     }}
  end

  def handle_call(:list, _from, state) do
    recent = Enum.to_list(state.recent)
    {:reply, {:ok, recent}, state}
  end

  def handle_call(:latest_le, _from, %{latest_log_event: nil} = state) do
    {:reply, {:error, :no_log_event_yet}, state}
  end

  def handle_call(:latest_le, _from, state) do
    {:reply, {:ok, state.latest_log_event}, state}
  end

  def handle_cast({:push, _source_id, [_le | _]} = msg, state) do
    {:noreply, do_push(msg, state)}
  end

  def handle_info({:stop_please, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info({:push, _source_id, [_le | _]} = msg, state) do
    {:noreply, do_push(msg, state)}
  end

  def handle_info(:broadcast, state) do
    {:ok, total_cluster_inserts, inserts_since_boot} = broadcast_count(state)

    broadcast()

    {:noreply,
     %{
       state
       | total_cluster_inserts: total_cluster_inserts,
         inserts_since_boot: inserts_since_boot
     }}
  end

  def handle_info(:touch, %{source_token: source_token} = state) do
    case Enum.to_list(state.recent) do
      [%Logflare.LogEvent{params: %{"is_system_log_event?" => true}}] ->
        touch()
        {:noreply, state}

      log_events ->
        log_event = Enum.reverse(log_events) |> hd()

        now = NaiveDateTime.utc_now()

        if NaiveDateTime.diff(now, log_event.ingested_at, :millisecond) < @touch_timer do
          Sources.Cache.get_by(token: source_token)
          |> Sources.update_source(%{log_events_updated_at: DateTime.utc_now()})
        end

        touch()
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _from, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.warning("[#{__MODULE__}] Unhandled message: #{inspect(message)}")

    {:noreply, state}
  end

  def terminate(reason, _state) do
    Logger.info("[#{__MODULE__}] Going Down: #{inspect(reason)}")
    reason
  end

  ## Private Functions
  defp do_push({:push, _source_token, [le | _] = log_events}, state) do
    new_queue = (Enum.to_list(state.recent) ++ log_events) |> LQueue.from_list(100)
    %{state | recent: new_queue, latest_log_event: le}
  end

  defp broadcast_count(
         %{source_token: source_token, inserts_since_boot: inserts_since_boot} = state
       ) do
    current_inserts = Source.Data.get_node_inserts(source_token)

    if current_inserts > inserts_since_boot do
      bq_inserts = Source.Data.get_bq_inserts(source_token)
      inserts_payload = %{Node.self() => %{node_inserts: current_inserts, bq_inserts: bq_inserts}}

      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "inserts",
        {:inserts, source_token, inserts_payload}
      )
    end

    current_cluster_inserts = PubSubRates.Cache.get_cluster_inserts(state.source_token)
    last_cluster_inserts = state.total_cluster_inserts

    if current_cluster_inserts > last_cluster_inserts do
      payload = %{log_count: current_cluster_inserts, source_token: state.source_token}
      Source.ChannelTopics.local_broadcast_log_count(payload)
    end

    {:ok, current_cluster_inserts, current_inserts}
  end

  def load_init_log_message(source_token) do
    message =
      "Initialized on node #{Node.self()}. Waiting for new events. Send some logs, then try to explore & search!"

    log_event =
      LE.make(
        %{
          "message" => message,
          "is_system_log_event?" => true
        },
        %{
          source: %Source{token: source_token}
        }
      )

    Process.send_after(self(), {:push, source_token, [log_event]}, 1_000)

    Source.ChannelTopics.broadcast_new(log_event)
  end

  defp touch() do
    rand = Enum.random(0..30) * :timer.minutes(1)

    every = rand + @touch_timer

    Process.send_after(self(), :touch, every)
  end

  defp broadcast() do
    Process.send_after(self(), :broadcast, @broadcast_every)
  end
end
