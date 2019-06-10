defmodule Logflare.Logs.RejectedEvents do
  alias Logflare.{Source, User}
  @cache __MODULE__

  @type rejected_log_event :: %{
          message: String.t(),
          payload: list(map) | map
        }

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, []]}}
  end

  @spec get_by_user(Logflare.User.t()) :: %{atom => list(rejected_log_event)}
  def get_by_user(%User{sources: sources}) do
    for source <- sources, into: Map.new() do
      {source.token, get_by_source(source)}
    end
  end

  @spec get_by_source(Logflare.Source.t()) :: list(rejected_log_event)
  def get_by_source(%Source{token: token}) do
    get!(token)
  end

  @doc """
  Expected to be called only in a log event params validation plug
  """
  @spec injest(%{log: map, source: Source.t()}) :: term
  def injest(%{log: log, source: %Source{token: token}}) do
    timestamp = log[:timestamp] || 1_000_000 * (Timex.now() |> Timex.to_unix())

    log = %{
      message: log.validation_error,
      log: log,
      timestamp: timestamp
    }

    insert(token, log)
  end

  defp get!(key) do
    {:ok, val} = Cachex.get(@cache, key)
    val
  end

  @spec insert(atom, map) :: list(map)
  defp insert(token, log) do
    Cachex.get_and_update!(@cache, token, fn
      xs when is_list(xs) ->
        Enum.take([log | xs], 100)

      _ ->
        [log]
    end)
  end
end
