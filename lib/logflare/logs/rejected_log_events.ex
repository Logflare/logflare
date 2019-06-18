defmodule Logflare.Logs.RejectedLogEvents do
  @moduledoc """
  Handles and caches LogEvents that failed validation
  """
  alias Logflare.{Source, User}
  alias Logflare.LogEvent, as: LE
  @cache __MODULE__

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  @spec get_by_user(Logflare.User.t()) :: %{atom => list(LE.t())}
  def get_by_user(%User{sources: sources}) do
    for source <- sources, into: Map.new() do
      {source.token, get_by_source(source)}
    end
  end

  @spec get_by_source(Source.t()) :: list(LE.t())
  def get_by_source(%Source{token: token}) do
    get!(token).log_events
    |> Enum.reverse()
  end

  def count(%Source{} = s) do
    s.token
    |> get!()
    |> Map.get(:count, 0)
  end

  @doc """
  Expected to be called only in Logs context
  """
  @spec injest(LE.t()) :: term
  def injest(%LE{source: %Source{}, valid?: false} = le) do
    insert(le.source.token, le)
  end

  @spec get!(atom) :: %{log_events: list(LE.t()), count: non_neg_integer}
  defp get!(key) do
    {:ok, val} = Cachex.get(@cache, key)
    val || %{log_events: [], count: 0}
  end

  @spec insert(atom, map) :: list(map)
  defp insert(token, log) when is_atom(token) do
    Cachex.get_and_update!(@cache, token, fn
      %{log_events: les, count: c} ->
        les =
          [log | les]
          |> List.flatten()
          |> Enum.take(100)

        %{log_events: les, count: c + 1}

      _ ->
        %{log_events: [log], count: 1}
    end)
  end
end
