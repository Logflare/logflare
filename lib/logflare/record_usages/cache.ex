defmodule Logflare.RecordUsages.Cache do
  @moduledoc """
  Buffers usage timestamps in Cachex on this node. Timestamps are UTC Unix seconds.

  The writer calls `put_if_newer/2` for each timestamp, skipping already-fresh values. Cachex timestamps are periodically persited to the database.
  """

  use Logflare.Cache

  import Cachex.Spec, only: [hook: 1]

  alias Logflare.Cache.CachexOps
  alias Logflare.RecordUsages.Writer

  @type key :: {module(), pos_integer()}
  @type entries :: [{key(), integer()}]

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_options) do
    hooks = if CachexOps.stats_enabled?(), do: [hook(module: Cachex.Stats)], else: []

    Supervisor.child_spec({Cachex, name: __MODULE__, ordered: true, hooks: hooks}, id: __MODULE__)
  end

  @spec put_if_newer(key(), integer()) :: {:ok, term()} | {:error, term()}
  def put_if_newer(key, timestamp) do
    update_timestamp = fn cache ->
      previous = Cachex.get!(cache, key)

      if is_nil(previous) or previous < timestamp do
        Cachex.put!(cache, key, timestamp)
      end
    end

    case Cachex.get(__MODULE__, key) do
      {:ok, previous} when is_nil(previous) or previous < timestamp ->
        Cachex.transaction(__MODULE__, [], update_timestamp)

      result ->
        result
    end
  end

  @spec stream(drain: boolean()) :: Enumerable.t()
  def stream(opts \\ []) do
    if Keyword.get(opts, :drain, true), do: Writer.drain()

    __MODULE__
    |> Cachex.stream!(Cachex.Query.build(output: {:key, :value}))
  end

  @spec acknowledge(entries()) :: :ok
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
      Cachex.transaction(__MODULE__, [], fn cache ->
        case Cachex.get!(cache, key) do
          ^timestamp -> Cachex.del!(cache, key)
          _ -> false
        end
      end)

    :ok
  end
end
