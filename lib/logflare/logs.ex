defmodule Logflare.Logs do
  use Publicist
  alias Logflare.Validator.{DeepFieldTypes, BigQuery}

  @dft_user_err "Metadata validation error: values with the same field path must have the same type."
  @bg_tm_user_err "Log entry metadata contains keys or values that are forbidden for the Google BigQuery table schema. Learn more at https://cloud.google.com/bigquery/docs/schemas"

  alias Logflare.{
    Table,
    TableCounter,
    SystemCounter,
    Sources,
    Source,
    SourceData,
    TableBuffer,
    Logs
  }

  alias Logflare.Logs.Injest

  alias Logflare.TableCounter
  alias Logflare.SystemCounter
  alias Number.Delimit

  @system_counter :total_logs_logged


  @spec insert_logs(list(map), Source.t()) :: :ok | {:error, term}
  def insert_logs(batch, %Source{} = source) when is_list(batch) do
    case validate_log_entries(batch) do
      :ok ->
        Enum.each(batch, &insert_log_to_source(&1, source))
        :ok

      {:invalid, reason} ->
        {:error, reason}
    end
  end
  
  @spec insert_or_push(atom(), {tuple(), map()}) :: true
  def insert_or_push(source_token, event) do
    if :ets.info(source_token) == :undefined do
      Table.push(source_token, event)
      true
    else
      :ets.insert(source_token, event)
    end
  end

  def broadcast_log_count(source_table) do
    {:ok, log_count} = TableCounter.get_total_inserts(source_table)
    source_table_string = Atom.to_string(source_table)

    payload = %{
      log_count: Delimit.number_to_delimited(log_count),
      source_token: source_table_string
    }

    LogflareWeb.Endpoint.broadcast(
      "dashboard:" <> source_table_string,
      "dashboard:#{source_table_string}:log_count",
      payload
    )
  end

  def broadcast_total_log_count() do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    payload = %{total_logs_logged: Delimit.number_to_delimited(log_count)}

    LogflareWeb.Endpoint.broadcast("everyone", "everyone:update", payload)
  end

  defp build_time_event(iso_datetime) when is_binary(iso_datetime) do
    monotime = System.monotonic_time(:nanosecond)

  @spec validate_log_entries(list(map)) :: :ok | {:invalid, term()}
  def validate_log_entries(batch) when is_list(batch) do
    Enum.reduce_while(
      batch,
      :ok,
      fn log_entry, _ ->
        case validate_log_entry(log_entry) do
          :ok -> {:cont, :ok}
          {:invalid, message} -> {:halt, {:error, message}}
        end
      end
    )
  end

  @spec validate_log_entry(map()) :: :ok | {:invalid, String.t()}
  def validate_log_entry(log_entry) when is_map(log_entry) do
    %{"metadata" => metadata} = log_entry

    with {:dft, true} <- {:dft, DeepFieldTypes.valid?(metadata)},
         {:bq_tm, true} <- {:bq_tm, BigQuery.TableMetadata.valid?(metadata)} do
      :ok
    else
      {:dft, false} -> {:invalid, @dft_user_err}
      {:bg_tm, false} -> {:invalid, @bg_tm_user_err}
    end
  end

    unix =
      iso_datetime
      |> Timex.parse!("{ISO:Extended}")
      |> Timex.to_unix()

    timestamp_mcs = unix * 1_000_000
    unique_int = System.unique_integer([:monotonic])
    {timestamp_mcs, unique_int, monotime}
  end
end
