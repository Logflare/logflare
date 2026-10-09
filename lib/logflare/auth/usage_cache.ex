defmodule Logflare.Auth.UsageCache do
  @moduledoc """
  Node-local pending token usage. Entries are retained until acknowledged after persistence.
  Unflushed usage is best-effort and does not survive loss of the node.
  """

  alias Logflare.Utils

  @type snapshot :: [{pos_integer(), DateTime.t()}]

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_options) do
    hooks =
      if Application.get_env(:logflare, :cache_stats, false), do: [Utils.cache_stats()], else: []

    Supervisor.child_spec({Cachex, name: __MODULE__, ordered: true, hooks: hooks}, id: __MODULE__)
  end

  @spec record(pos_integer(), DateTime.t()) :: :ok
  def record(id, timestamp) do
    Cachex.get_and_update!(__MODULE__, id, fn
      nil -> {:commit, timestamp}
      previous -> {:commit, Enum.max([previous, timestamp], DateTime)}
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

  @spec acknowledge_entry({pos_integer(), DateTime.t()}) :: :ok
  defp acknowledge_entry({id, timestamp}) do
    {:ok, _} =
      Cachex.transaction(__MODULE__, [id], fn cache ->
        case Cachex.get!(cache, id) do
          ^timestamp -> Cachex.del!(cache, id)
          _ -> false
        end
      end)

    :ok
  end
end
