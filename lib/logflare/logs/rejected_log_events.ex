defmodule Logflare.Logs.RejectedLogEvents do
  @moduledoc """
  Handles and caches LogEvents that failed validation. To genereate a rejected log:
  `Logger.info("should be rejected", users_1: [%{id: "1"}, %{id: 1}] )`

  or

  ```
  curl -X "POST" "http://localhost:4000/logs/cloudflare" \
    -H 'Content-Type: application/json' \
    -H 'X-API-KEY: EdR8jNi258ji' \
    -d $'{
      "metadata": {
        "users": [
          {"id": "1"},
          {"id": 1}
        ]
      },
      "log_entry": "should be rejected",
      "source": "09f5db03-ac00-44fa-80b5-26a531e09524"
    }'
  ```
  """
  use Logflare.Commons
  alias RejectedLogEvent, as: RLE
  import Ecto.Query

  @spec get_for_user(Logflare.User.t()) :: %{atom => list(RLE.t())}
  def get_for_user(%User{sources: sources}) do
    for source <- sources, into: Map.new() do
      {source.token, get_for_source(source)}
    end
  end

  @spec get_for_source(Source.t()) :: list(RLE.t())
  def get_for_source(%Source{token: token}) do
    get!(token)
  end

  def count(%Source{} = s) do
    from(RejectedLogEvent)
    |> where([rle], rle.source_id == ^s.id)
    |> select([rle], rle.id)
    |> RepoWithCache.all()
    |> Enum.count()
  end

  def delete_by_source(%Source{token: token}) do
    s = Sources.get_by(token: token)

    {_, _} =
      RepoWithCache.delete_all(from(RejectedLogEvent) |> where([rle], rle.source_id == ^s.id))

    {:ok, true}
  end

  @doc """
  Expected to be called only in Logs context
  """
  @spec ingest(LE.t()) :: :ok
  def ingest(%LE{source: %Source{id: id}, valid: false} = le) do
    {:ok, _rle} =
      RepoWithCache.insert(%RejectedLogEvent{
        source_id: id,
        params: le.params,
        ingested_at: le.ingested_at,
        # ingested_at: DateTime.from_unix!(le.ingested_at, :microsecond),
        validation_error: le.validation_error
      })

    :ok
  end

  @spec get!(atom) :: list(RLE.t())
  defp get!(token) do
    s = Sources.get_by(token: token)
    RepoWithCache.all(from(RejectedLogEvent) |> where([rle], rle.source_id == ^s.id))
  end
end
