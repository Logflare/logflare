defmodule Logflare.Logs.RejectedLogEvents do
  @moduledoc """
  Handles and caches LogEvents that failed validation. To genereate a rejected log:
  `Logger.info("should be rejected", users_1: [%{id: "1"}, %{id: 1}] )`

  or

  ```
  curl -X "POST" "http://localhost:4000/logs/cloudflare" \
    -H 'Content-Type: application/json' \
    -H 'X-API-KEY: ZvQ2p6Rf-TbR' \
    -d $'{
      "metadata": {
        "users": [
          {"id": "1"},
          {"id": 1}
        ]
      },
      "log_entry": "should be rejected",
      "source": "1a5c639d-1e1c-4e2f-ae24-60af0c10e654"
    }'
  ```
  """
  alias Logflare.Source
  alias Logflare.LogEvent, as: LE

  @cache __MODULE__

  def child_spec(_) do
    %{id: @cache, start: {Cachex, :start_link, [@cache, [limit: 10_000]]}}
  end

  @spec get_by_source(Source.t()) :: list(LE.t())
  def get_by_source(%Source{token: token}) do
    get!(token).log_events
  end

  def count(%Source{} = s) do
    count = Cachex.get!(@cache, counter_name(s.token))
    count || 0
  end

  def delete_by_source(%Source{token: token}) do
    results = query(token)

    for r <- results do
      {:ok, true} = Cachex.del(@cache, {token, r.id})
      Cachex.decr(@cache, counter_name(token))
    end

    {:ok, true}
  end

  @doc """
  Expected to be called only in Logs context
  """
  @spec ingest(LE.t()) :: :ok
  def ingest(%LE{source: %Source{token: token}, valid?: false, id: id} = le) do
    Cachex.put!(@cache, {token, id}, le)
    Cachex.incr(@cache, counter_name(token))

    :ok
  end

  def query(source_id) when is_atom(source_id) do
    @cache
    |> Cachex.stream!()
    |> Enum.filter(fn x ->
      case x do
        {:entry, {^source_id, _le_id}, _ts, _, _le} ->
          true

        _ ->
          false
      end
    end)
    |> Enum.map(fn x ->
      case x do
        {:entry, {^source_id, _le_id}, _ts, _, le} ->
          le
      end
    end)
    |> Enum.reverse()
  end

  @spec get!(atom) :: %{log_events: list(LE.t()), count: non_neg_integer}
  defp get!(key) do
    events = query(key)
    count = Cachex.get!(@cache, counter_name(key))

    case events do
      [] ->
        %{log_events: [], count: 0}

      events ->
        %{log_events: events, count: count || 0}
    end
  end

  defp counter_name(token) do
    "#{token}-count"
  end
end
