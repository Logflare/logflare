defmodule Logflare.Google.BigQuery.Query do
  require Logger

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id] || ""
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append] || ""

  alias GoogleApi.BigQuery.V2.{Api, Model}
  alias Logflare.Google.BigQuery.{GenUtils}
  alias Logflare.SourceBigQuerySchema
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  @spec get_events_for_ets(atom, atom) :: []
  def get_events_for_ets(
        source,
        project_id \\ @project_id,
        datetime \\ nil,
        streaming_buffer \\ false
      ) do
    conn = GenUtils.get_conn()
    sql = gen_sql_for_ets(source, project_id, datetime, streaming_buffer)

    case query(conn, project_id, sql) do
      {:ok, response} ->
        if response.rows do
          Logflare.Google.BigQuery.SchemaUtils.merge_rows_with_schema(
            response.schema,
            response.rows
          )
          |> Enum.reverse()
          |> Enum.map(fn row ->
            unique_integer = System.unique_integer([:monotonic])
            time_event = {0, unique_integer, 0}
            payload = string_keys_to_atoms(row)

            {time_event, payload}
          end)
        else
          []
        end

      {:error, _response} ->
        Logger.error("BigQuery error. Likely job permissions. Source: #{source}")
        unique_integer = System.unique_integer([:monotonic])
        time_event = {0, unique_integer, 0}

        payload = %{
          timestamp: System.system_time(:microsecond),
          log_message:
            "If you're seeing this there was an issue populating the cache from
            BigQuery. This doesn't affect log ingestion. It could be a temporary
            timeout. If this message persists and you have your own BigQuery backed
            setup you may need to add `BigQuery Job User` permissions to the
            Logflare service account via Google CLoud Platform console IAM.
            See: #{Routes.marketing_url(Endpoint, :big_query_setup)} or email support@logflare.app if you need help."
        }

        [{time_event, payload}]
    end
  end

  defp query(conn, project_id, sql) do
    Api.Jobs.bigquery_jobs_query(
      conn,
      project_id,
      body: %Model.QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: true
      }
    )
  end

  defp gen_sql_for_ets(source, project_id, datetime, streaming_buffer) do
    table_name = GenUtils.format_table_name(source)
    streaming_buffer_sql = "IS NULL"
    day = "#{datetime.year}-#{datetime.month}-#{datetime.day}"
    dataset_id = GenUtils.get_account_id(source) <> @dataset_id_append
    schema = SourceBigQuerySchema.get_state(source).schema
    has_metadata? = Enum.find_value(schema.fields, fn x -> x.name == "metadata" end)

    # To properly do the JSON we have to recursivly unnest the metadata based on the schema we have

    if streaming_buffer do
      if has_metadata? do
        "SELECT timestamp, event_message as log_message, m as metadata FROM `#{project_id}.#{
          dataset_id
        }.#{table_name}`, UNNEST(metadata) m WHERE DATE(_PARTITIONTIME) #{streaming_buffer_sql} ORDER BY timestamp DESC LIMIT 100"
      else
        "SELECT timestamp, event_message as log_message FROM `#{project_id}.#{dataset_id}.#{
          table_name
        }` WHERE DATE(_PARTITIONTIME) #{streaming_buffer_sql} ORDER BY timestamp DESC LIMIT 100"
      end
    else
      if has_metadata? do
        "SELECT timestamp, event_message as log_message, m as metadata FROM `#{project_id}.#{
          dataset_id
        }.#{table_name}`, UNNEST(metadata) m WHERE DATE(_PARTITIONTIME) = \"#{day}\" ORDER BY timestamp DESC LIMIT 100"
      else
        "SELECT timestamp, event_message as log_message FROM `#{project_id}.#{dataset_id}.#{
          table_name
        }` WHERE DATE(_PARTITIONTIME) = \"#{day}\" ORDER BY timestamp DESC LIMIT 100"
      end
    end
  end

  @spec string_keys_to_atoms(map) :: map
  defp string_keys_to_atoms(payload) do
    for {key, val} <- payload, into: %{}, do: {String.to_atom(key), val}
  end
end
