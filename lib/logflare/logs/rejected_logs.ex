defmodule Logflare.Logs.RejectedEvents do
  alias Logflare.{Source, User}
  @cache __MODULE__
  import Cachex.Spec

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  @spec get_by_user(Logflare.User.t()) :: map
  def get_by_user(%User{sources: sources}) do
    for source <- sources, into: Map.new() do
      {source.token, get_by_source(source)}
    end
  end

  @spec get_by_source(Logflare.Source.t()) :: map
  def get_by_source(%Source{token: token}) do
    get!(token)
  end

  @doc """
  Expected to be called only in a log event params validation plug
  """
  def injest(%{
        reason: reason,
        log_events: log_events,
        source: source
      }) do
    insert(source, reason, log_events)
  end

  defp get!(key) do
    {:ok, val} = Cachex.get(@cache, key)
    val
  end

  @spec insert(Logflare.Source.t(), atom, list(map)) :: map
  def insert(%Source{token: token}, error, value) do
    Cachex.get_and_update!(@cache, token, fn
      %{^error => logs} = val -> %{val | error => Enum.take([value | logs], 100)}
      map -> Map.put(map || %{}, error, value)
    end)
  end
end
