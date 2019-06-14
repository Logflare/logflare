defmodule Logflare.Logs.RejectedLogEvents do
  alias Logflare.{Source, User}
  alias Logflare.LogEvent
  @cache __MODULE__

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  @spec get_by_user(Logflare.User.t()) :: %{atom => list(LogEvent.t())}
  def get_by_user(%User{sources: sources}) do
    for source <- sources, into: Map.new() do
      {source.token, get_by_source(source)}
    end
  end

  @spec get_by_source(Logflare.Source.t()) :: list(LogEvent.t())
  def get_by_source(%Source{token: token}) do
    get!(token)
  end

  @doc """
  Expected to be called only in Logs context
  """
  @spec injest(LogEvent.t()) :: term
  def injest(%LogEvent{source: %Source{}, valid?: false} = le) do
    insert(le.source.token, le)
  end

  defp get!(key) do
    {:ok, val} = Cachex.get(@cache, key)
    val
  end

  @spec insert(atom, map) :: list(map)
  defp insert(token, log) when is_atom(token) do
    Cachex.get_and_update!(@cache, token, fn
      xs when is_list(xs) ->
        Enum.take([log | xs], 100)

      _ ->
        [log]
    end)
  end
end
