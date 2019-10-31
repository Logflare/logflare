defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.{Source, Sources, EctoQueryBQ}
  alias Logflare.Logs.Search.Parser
  import Ecto.Query

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest

  use Logflare.GenDecorators
  import Logflare.Logs.SearchOperations

  alias Logflare.Logs.SearchOperations.SearchOperation, as: SO

  def search(%SO{} = so) do
    so
    |> Map.put(:stats, %{
      start_monotonic_time: System.monotonic_time(:millisecond),
      total_duration: nil
    })
    |> default_from
    |> parse_querystring()
    |> verify_path_in_schema()
    |> apply_local_timestamp_correction()
    |> partition_or_streaming()
    |> apply_wheres()
    |> apply_selects()
    |> order_by_default()
    |> apply_limit_to_query()
    |> apply_to_sql()
    |> do_query()
    |> process_query_result()
    |> put_stats()
    |> case do
      %{error: nil} = so ->
        {:ok, so}

      so ->
        {:error, so}
    end
  end

end
