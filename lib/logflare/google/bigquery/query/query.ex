defmodule Logflare.Google.BigQuery.Query do
  @moduledoc false
  require Logger

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  alias GoogleApi.BigQuery.V2.{Api, Model}
  alias Logflare.Google.BigQuery.{GenUtils}
  alias Logflare.SourceBigQuerySchema
  alias Logflare.Google.BigQuery
  #
  #  @spec get_events_for_ets(atom, atom) :: []
  #  def get_events_for_ets(
  #        source,
  #        project_id \\ @project_id
  #      ) do
  #    schema = SourceBigQuerySchema.get_state(source).schema_not_sorted
  #
  #    case list_table_rows(source, project_id) do
  #      {:ok, response} ->
  #        if response.rows do
  #          BigQuery.SchemaUtils.merge_rows_with_schema(
  #            schema,
  #            response.rows
  #          )
  #          |> Enum.map(fn row ->
  #            %{"timestamp" => timestamp, "event_message" => log_message, "metadata" => metadata} =
  #              row
  #
  #            unique_integer = System.unique_integer([:monotonic])
  #            time_event = {timestamp, unique_integer, 0}
  #
  #            {time_event, %{timestamp: timestamp, log_message: log_message, metadata: metadata}}
  #          end)
  #        else
  #          []
  #        end
  #
  #      {:error, _response} ->
  #        []
  #    end
  #  end
  #
  #  def list_table_rows(
  #        source,
  #        project_id \\ @project_id
  #      ) do
  #    conn = GenUtils.get_conn()
  #    dataset_id = GenUtils.get_account_id(source) <> @dataset_id_append
  #    table_name = GenUtils.format_table_name(source)
  #
  #    Api.Tabledata.bigquery_tabledata_list(conn, project_id, dataset_id, table_name,
  #      maxResults: 100,
  #      alt: "JSON",
  #      selectedFields: "event_message, metadata, timestamp"
  #    )
  #  end

  def query(conn, project_id, sql, params) do
    Api.Jobs.bigquery_jobs_query(
      conn,
      project_id,
      body: %Model.QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: true,
        parameterMode: "NAMED",
        queryParameters: params
      }
    )
  end

  #
  #  defp gen_sql_for_ets(source, project_id, datetime, streaming_buffer) do
  #    table_name = GenUtils.format_table_name(source)
  #    streaming_buffer_sql = "IS NULL"
  #    day = "#{datetime.year}-#{datetime.month}-#{datetime.day}"
  #    dataset_id = GenUtils.get_account_id(source) <> @dataset_id_append
  #    schema = SourceBigQuerySchema.get_state(source).schema
  #    has_metadata? = Enum.find_value(schema.fields, fn x -> x.name == "metadata" end)
  #
  #    # To properly do the JSON we have to recursivly unnest the metadata based on the schema we have
  #
  #    if streaming_buffer do
  #      if has_metadata? do
  #        "SELECT timestamp, event_message as log_message, m as metadata FROM `#{project_id}.#{
  #          dataset_id
  #        }.#{table_name}`, UNNEST(metadata) m WHERE DATE(_PARTITIONTIME) #{streaming_buffer_sql} ORDER BY timestamp DESC LIMIT 100"
  #      else
  #        "SELECT timestamp, event_message as log_message FROM `#{project_id}.#{dataset_id}.#{
  #          table_name
  #        }` WHERE DATE(_PARTITIONTIME) #{streaming_buffer_sql} ORDER BY timestamp DESC LIMIT 100"
  #      end
  #    else
  #      if has_metadata? do
  #        "SELECT timestamp, event_message as log_message, m as metadata FROM `#{project_id}.#{
  #          dataset_id
  #        }.#{table_name}`, UNNEST(metadata) m WHERE DATE(_PARTITIONTIME) = \"#{day}\" ORDER BY timestamp DESC LIMIT 100"
  #      else
  #        "SELECT timestamp, event_message as log_message FROM `#{project_id}.#{dataset_id}.#{
  #          table_name
  #        }` WHERE DATE(_PARTITIONTIME) = \"#{day}\" ORDER BY timestamp DESC LIMIT 100"
  #      end
  #    end
  #  end
  #
  #  @spec string_keys_to_atoms(map) :: map
  #  defp string_keys_to_atoms(payload) do
  #    for {key, val} <- payload, into: %{}, do: {String.to_atom(key), val}
  #  end
end
