defmodule Logflare.RecordUsages.Cache do
  @moduledoc """
  Node-local cache to buffer record usage.
  Timestamps are UTC Unix seconds.
  Entries are retained until persisted (see `Logflare.RecordUsages.SyncWorker` and `Logflare.RecordUsages.ShutdownWorker`).
  """

  use Logflare.Cache

  import Cachex.Spec, only: [hook: 1]

  alias Logflare.Cache.CachexOps

  @type key :: {module(), pos_integer()}
  @type snapshot :: [{key(), integer()}]

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_options) do
    hooks = if CachexOps.stats_enabled?(), do: [hook(module: Cachex.Stats)], else: []

    Supervisor.child_spec({Cachex, name: __MODULE__, ordered: true, hooks: hooks}, id: __MODULE__)
  end

  @spec record(key(), integer()) :: :ok
  def record(key, timestamp) do
    Cachex.get_and_update!(__MODULE__, key, fn
      nil -> {:commit, timestamp}
      previous -> {:commit, max(previous, timestamp)}
    end)

    :ok
  end

  @spec snapshot() :: snapshot()
  def snapshot do
    __MODULE__
    |> Cachex.stream!(Cachex.Query.build(output: {:key, :value}))
    |> Enum.to_list()
  end

  @spec acknowledge(snapshot()) :: :ok
  def acknowledge(entries) do
    Enum.each(entries, &acknowledge_entry/1)
  end

  @spec acknowledge_entry({key(), integer()}) :: :ok
  defp acknowledge_entry({key, timestamp}) do
    {:ok, _} =
      Cachex.transaction(__MODULE__, [key], fn cache ->
        case Cachex.get!(cache, key) do
          ^timestamp -> Cachex.del!(cache, key)
          _ -> false
        end
      end)

    :ok
  end
end
