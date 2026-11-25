defmodule Logflare.RecordUsages.Cache do
  @moduledoc """
  Buffers usage timestamps in Cachex on this node. Timestamps are UTC Unix seconds.

  A writer queues up timestamps and periodically writes to Cachex.
  Restarting the node loses pending samples. Values already in Cachex remain.

  Cachex timestamps are periodically persited to the database.
  """

  use Logflare.Cache

  import Cachex.Spec, only: [hook: 1]

  alias Logflare.Cache.CachexOps
  alias Logflare.RecordUsages.Writer

  @type key :: {module(), pos_integer()}
  @type snapshot :: [{key(), integer()}]

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_options) do
    hooks = if CachexOps.stats_enabled?(), do: [hook(module: Cachex.Stats)], else: []

    Supervisor.child_spec({Cachex, name: __MODULE__, ordered: true, hooks: hooks}, id: __MODULE__)
  end

  @spec record(key(), integer()) :: :ok
  def record(key, timestamp) do
    case Cachex.get(__MODULE__, key) do
      {:ok, previous} when is_integer(previous) and previous >= timestamp ->
        :ok

      {:ok, _} ->
        Writer.record(key, timestamp)

      {:error, _reason} ->
        :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec snapshot(drain: boolean()) :: snapshot()
  def snapshot(opts \\ []) do
    if Keyword.get(opts, :drain, true), do: Writer.drain()

    __MODULE__
    |> Cachex.stream!(Cachex.Query.build(output: {:key, :value}))
    |> Enum.to_list()
  end

  @spec acknowledge(snapshot()) :: :ok
  def acknowledge(entries) do
    Enum.each(entries, &acknowledge_entry/1)
  end

  @impl Logflare.Cache
  @spec reset() :: :ok
  def reset do
    Writer.drain()
    CachexOps.reset(__MODULE__)
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
