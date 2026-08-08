defmodule Logflare.LogEvent do
  use TypedEctoSchema

  import Ecto.Changeset

  import Logflare.Utils.Guards,
    only: [is_non_empty_binary: 1, is_non_negative_integer: 1, is_pos_integer: 1]

  alias __MODULE__, as: LE
  alias __MODULE__.DayBucket
  alias __MODULE__.TypeDetection
  alias Logflare.KeyValues
  alias Logflare.Logs.Ingest.MetadataCleaner
  alias Logflare.Logs.IngestTransformers
  alias Logflare.Logs.Validators.BigQuerySchemaChange
  alias Logflare.Sources.Source
  alias Logflare.Utils

  @validators [BigQuerySchemaChange]

  @primary_key {:id, :binary_id, []}
  typed_embedded_schema do
    field :body, :map, default: %{}
    field :valid, :boolean
    field :drop, :boolean, default: false
    field :is_from_stale_query, :boolean
    field :timestamp_inferred, :boolean, default: false
    field :ingested_at, :utc_datetime_usec
    field :source_uuid, Ecto.UUID.Atom
    field :source_name, :string
    field :via_rule_id, :id
    field :retries, :integer, default: 0
    field :event_type, Ecto.Enum, values: [:log, :metric, :trace], default: :log
    field :source_id, :integer, default: nil
    field :day_bucket, :integer
    # Indicates if the event was removed from ets during ingest
    field :is_popped, :boolean, virtual: true, default: false

    embeds_one :pipeline_error, PipelineError do
      field :stage, :string
      field :type, :string
      field :message, :string
    end
  end

  @doc """
  Reconstructs a LogEvent from a record stored in the spool by the producer pipeline.
  Skips the full make/transform/validate pipeline — the body is already in
  BQ column spec format and the event was already validated on ingest.

  Handles both NDJSON (string keys, ISO8601 ingested_at) and ETF (atom keys,
  native DateTime) formats written by the producer.
  """
  @spec make_from_spool(map(), Source.t()) :: t()
  def make_from_spool(
        %{
          id: id,
          body: body,
          event_type: event_type,
          ingested_at: ingested_at_us
        } = record,
        source
      )
      when is_integer(ingested_at_us) do
    ingested_at_dt = DateTime.from_unix!(ingested_at_us, :microsecond)
    day_bucket = body["timestamp"] && DayBucket.from_microseconds(body["timestamp"])

    %__MODULE__{
      id: id,
      source_id: source.id,
      source_uuid: source.token,
      source_name: source.name,
      body: body,
      event_type: event_type,
      ingested_at: ingested_at_dt,
      valid: true,
      drop: false,
      day_bucket: day_bucket,
      via_rule_id: Map.get(record, :via_rule_id)
    }
  end

  def make_from_spool(
        %{
          "id" => id,
          "body" => body,
          "event_type" => event_type,
          "ingested_at" => ingested_at
        } = record,
        source
      ) do
    {:ok, ingested_at_dt, _} = DateTime.from_iso8601(ingested_at)
    day_bucket = body["timestamp"] && DayBucket.from_microseconds(body["timestamp"])

    %__MODULE__{
      id: id,
      source_id: source.id,
      source_uuid: source.token,
      source_name: source.name,
      body: body,
      event_type: String.to_existing_atom(event_type),
      ingested_at: ingested_at_dt,
      valid: true,
      drop: false,
      day_bucket: day_bucket,
      via_rule_id: Map.get(record, "via_rule_id")
    }
  end

  @doc """
  Used to generate log events from bigquery rows.
  """
  @spec make_from_db(map(), %{source: Source.t()}) :: LE.t()
  def make_from_db(params, %{source: %Source{} = source}) do
    params =
      params
      |> mapper_from_db(:log)

    %__MODULE__{}
    |> cast(params, [:valid, :id, :body])
    |> cast_embed(:pipeline_error, with: &pipeline_error_changeset/2)
    |> apply_changes()
    |> Map.put(:source_id, source.id)
  end

  @doc """
  Used to make log event from user-provided parameters, for ingestion.
  """
  @spec make(%{optional(String.t()) => term}, %{source: Source.t()}) :: LE.t()
  def make(
        params,
        %{
          source: %Source{id: source_id, token: source_uuid, name: source_name} = source
        }
      ) do
    event_type = TypeDetection.detect(params)

    %{
      "body" => %{"id" => id, "timestamp" => timestamp} = body,
      "timestamp_inferred" => timestamp_inferred
    } = mapper_for_ingest(params, event_type)

    day_bucket = DayBucket.from_microseconds(timestamp)
    ingested_at = DateTime.utc_now()
    body = transform_body(body, source)

    %__MODULE__{
      body: body,
      source_id: source_id,
      source_uuid: source_uuid,
      source_name: source_name,
      valid: true,
      ingested_at: ingested_at,
      id: id,
      event_type: event_type,
      timestamp_inferred: timestamp_inferred,
      day_bucket: day_bucket
    }
    |> validate(source)
  end

  @spec mapper_from_db(map(), TypeDetection.event_type()) :: %{String.t() => term}
  defp mapper_from_db(params, event_type),
    do: mapper(params, event_type, &MetadataCleaner.deep_reject_nil_and_empty/1)

  @spec mapper_for_ingest(map(), TypeDetection.event_type()) :: %{String.t() => term}
  defp mapper_for_ingest(params, event_type),
    do: mapper(params, event_type, &clean_ingest_body/1)

  @spec mapper(map(), TypeDetection.event_type(), (map() -> map())) :: %{String.t() => term}
  defp mapper(params, event_type, clean_body) do
    # TODO: deprecate and remove `message`
    event_message = params["message"] || params["event_message"]
    id = id(params)

    {timestamp, timestamp_inferred} = determine_timestamp(params, event_type)

    base_merge = %{
      "timestamp" => timestamp,
      "id" => id
    }

    base_merge =
      if event_message != nil do
        Map.put(base_merge, "event_message", event_message)
      else
        base_merge
      end

    body =
      params
      |> clean_body.()
      |> Map.merge(base_merge)
      |> case do
        %{"message" => m, "event_message" => em} = map when m == em ->
          Map.delete(map, "message")

        other ->
          other
      end

    %{
      "body" => body,
      "id" => id,
      "timestamp_inferred" => timestamp_inferred
    }
  end

  @spec validate(LE.t(), Source.t()) :: LE.t()
  defp validate(%LE{valid: true} = le, source) do
    @validators
    |> Enum.reduce_while(true, fn validator, _acc ->
      case validator.validate(le, source) do
        :ok ->
          {:cont, %{le | valid: true, pipeline_error: nil}}

        {:error, message} ->
          {:halt,
           %{
             le
             | valid: false,
               pipeline_error: %LE.PipelineError{
                 stage: "validators",
                 type: "validate",
                 message: message
               }
           }}
      end
    end)
  end

  @spec clean_ingest_body(map()) :: map()
  defp clean_ingest_body(params),
    do: IngestTransformers.transform(params, :clean_to_bigquery_column_spec)

  @spec transform_body(map(), Source.t()) :: map()
  defp transform_body(body, %Source{
         transform_copy_fields: copy_config,
         transform_copy_fields_parsed: copy_parsed,
         transform_key_values: kv_config,
         transform_key_values_parsed: kv_parsed,
         transform_drop_fields: drop_config,
         transform_drop_fields_parsed: drop_parsed
       })
       when (copy_parsed == [] or (is_nil(copy_parsed) and copy_config in [nil, ""])) and
              (kv_parsed == [] or (is_nil(kv_parsed) and kv_config in [nil, ""])) and
              (drop_parsed == [] or (is_nil(drop_parsed) and drop_config in [nil, ""])),
       do: body

  defp transform_body(body, %Source{} = source) do
    body
    |> copy_fields(source)
    |> kv_enrich(source)
    |> drop_fields(source)
  end

  @spec copy_fields(map(), Source.t()) :: map()
  defp copy_fields(body, %Source{
         transform_copy_fields_parsed: nil,
         transform_copy_fields: blank
       })
       when blank in [nil, ""],
       do: body

  defp copy_fields(body, %Source{transform_copy_fields_parsed: []}), do: body

  defp copy_fields(body, %Source{transform_copy_fields_parsed: parsed})
       when is_list(parsed) do
    Enum.reduce(parsed, body, fn %{from_path: from_path, to_path: to_path}, acc ->
      case get_at_path(acc, from_path) do
        nil -> acc
        value -> put_at_path(acc, to_path, value)
      end
    end)
  end

  defp copy_fields(body, %Source{} = source) do
    copy_fields(body, Source.parse_copy_fields_config(source))
  end

  @spec kv_enrich(map(), Source.t()) :: map()
  defp kv_enrich(body, %Source{
         transform_key_values: blank,
         transform_key_values_parsed: nil
       })
       when blank in [nil, ""],
       do: body

  defp kv_enrich(body, %Source{transform_key_values_parsed: []}), do: body

  defp kv_enrich(body, %Source{transform_key_values_parsed: parsed, user_id: user_id})
       when is_list(parsed) do
    Enum.reduce(parsed, body, fn instruction, acc ->
      apply_kv_instruction(acc, instruction, user_id)
    end)
  end

  # Fallback: parse at ingestion time when parsed field is not populated
  defp kv_enrich(body, %Source{} = source) do
    kv_enrich(body, Source.parse_key_values_config(source))
  end

  @spec apply_kv_instruction(map(), map(), integer()) :: map()
  defp apply_kv_instruction(
         body,
         %{from_path: from_path, to_path: to_path} = instruction,
         user_id
       ) do
    accessor_path = Map.get(instruction, :accessor_path)

    with raw when not is_nil(raw) <- get_at_path(body, from_path),
         raw_string <- to_string(raw),
         true <- Utils.flag("key_values", raw_string),
         value when not is_nil(value) <-
           KeyValues.Cache.lookup(user_id, raw_string, accessor_path) do
      put_at_path(body, to_path, value)
    else
      _ -> body
    end
  end

  defp get_at_path(nil, _path), do: nil

  defp get_at_path(map, [key]) when is_map(map) and is_binary(key),
    do: Map.get(map, key)

  defp get_at_path(map, [head | tail]) when is_map(map) and is_binary(head),
    do: get_at_path(Map.get(map, head), tail)

  defp get_at_path(data, path), do: get_in(data, path)

  @spec put_at_path(map(), [String.t()], term()) :: map()
  defp put_at_path(map, [leaf], value) when is_map(map), do: Map.put(map, leaf, value)

  defp put_at_path(map, [head | rest], value) when is_map(map) do
    child = Map.get(map, head, %{})
    Map.put(map, head, put_at_path(child, rest, value))
  end

  @spec drop_fields(map(), Source.t()) :: map()
  defp drop_fields(body, %Source{
         transform_drop_fields_parsed: nil,
         transform_drop_fields: blank
       })
       when blank in [nil, ""],
       do: body

  defp drop_fields(body, %Source{transform_drop_fields_parsed: []}), do: body

  defp drop_fields(body, %Source{transform_drop_fields_parsed: parsed})
       when is_list(parsed) do
    Enum.reduce(parsed, body, fn keys, acc -> drop_field_at(acc, keys) end)
  end

  defp drop_fields(body, %Source{} = source) do
    drop_fields(body, Source.parse_drop_fields_config(source))
  end

  @spec drop_field_at(term(), [String.t()]) :: term()
  defp drop_field_at(body, [leaf]) when is_map(body), do: Map.delete(body, leaf)

  defp drop_field_at(body, [head | rest]) when is_map(body) do
    case Map.fetch(body, head) do
      {:ok, child} when is_map(child) ->
        case drop_field_at(child, rest) do
          ^child -> body
          updated_child -> Map.put(body, head, updated_child)
        end

      _ ->
        body
    end
  end

  defp drop_field_at(body, _), do: body

  @doc """
  Generates a custom event message from source settings.any()

  The `:custom_event_message_keys` key on the source determines what values are extracted from the log event body and set into the `event_message` key.

  Configuration should be comma separated, and it accepts json query syntax.
  """
  @spec apply_custom_event_message(LE.t(), Source.t()) :: LE.t()
  def apply_custom_event_message(%LE{drop: true} = le, _source), do: le

  def apply_custom_event_message(%LE{} = le, %Source{} = source) do
    message = make_message(le, source)

    le
    |> Kernel.put_in([Access.key(:body), "event_message"], message)
  end

  @doc """
  Changeset for pipeline errors.
  """
  @spec pipeline_error_changeset(LE.PipelineError.t(), map()) :: Ecto.Changeset.t()
  def pipeline_error_changeset(pipeline_error, attrs) do
    pipeline_error
    |> cast(attrs, [
      :stage,
      :message
    ])
    |> validate_required([:stage, :message])
  end

  @spec make_message(LE.t(), Source.t()) :: String.t() | nil
  defp make_message(log_event, source) do
    if keys = source.custom_event_message_keys do
      keys
      |> String.split(",", trim: true)
      |> Enum.map_join(" | ", fn key -> build_message(key, log_event) end)
    else
      get_default_message(log_event)
    end
  end

  @spec build_message(String.t(), LE.t()) :: String.t() | nil
  defp build_message(key, log_event) do
    message = get_default_message(log_event)

    case String.trim(key) do
      "id" ->
        to_string(log_event.id)

      "message" ->
        message

      "event_message" ->
        message

      "m." <> rest ->
        query_json(log_event.body, "$.metadata.#{rest}")

      keys ->
        query_json(log_event.body, "$.#{keys}")
    end
  end

  @spec get_default_message(LE.t()) :: String.t() | nil
  defp get_default_message(log_event) do
    log_event.body["message"] || log_event.body["event_message"]
  end

  @spec query_json(map(), String.t()) :: String.t()
  defp query_json(metadata, query) do
    case Warpath.query(metadata, query) do
      {:ok, v} ->
        Jason.encode!(v)

      {:error, _} ->
        "json_path_query_error"
    end
  end

  @spec id(map()) :: String.t()
  defp id(params) do
    params["id"] || params[:id] || Ecto.UUID.generate()
  end

  @spec determine_timestamp(map(), TypeDetection.event_type()) :: {integer(), boolean()}
  defp determine_timestamp(params, event_type) do
    params
    |> determine_timestamp()
    |> maybe_use_trace_start_time(event_type, params)
  end

  @spec determine_timestamp(map()) :: {integer(), boolean()}
  defp determine_timestamp(params) when not is_map_key(params, "timestamp"),
    do: {default_timestamp(), true}

  defp determine_timestamp(%{"timestamp" => x}) when is_non_empty_binary(x) do
    case DateTime.from_iso8601(x) do
      {:ok, udt, _} ->
        {DateTime.to_unix(udt, :microsecond), false}

      {:error, _} ->
        {default_timestamp(), true}
    end
  end

  defp determine_timestamp(%{"timestamp" => x}) when is_non_negative_integer(x) do
    {timestamp_to_microseconds(x), false}
  end

  defp determine_timestamp(%{"timestamp" => x}) when is_float(x) do
    determine_timestamp(%{"timestamp" => round(x)})
  end

  defp determine_timestamp(_), do: {default_timestamp(), true}

  @spec maybe_use_trace_start_time(
          {integer(), boolean()},
          TypeDetection.event_type(),
          map()
        ) :: {integer(), boolean()}
  defp maybe_use_trace_start_time({_, true} = default, :trace, params) do
    case extract_trace_start_time(params) do
      nil -> default
      start_time_us -> {start_time_us, true}
    end
  end

  defp maybe_use_trace_start_time(result, _event_type, _params), do: result

  @spec extract_trace_start_time(params :: map()) :: pos_integer() | nil
  defp extract_trace_start_time(%{"start_time" => n}) when is_pos_integer(n),
    do: timestamp_to_microseconds(n)

  defp extract_trace_start_time(%{"startTime" => n}) when is_pos_integer(n),
    do: timestamp_to_microseconds(n)

  defp extract_trace_start_time(%{"start_time_unix_nano" => n}) when is_pos_integer(n),
    do: timestamp_to_microseconds(n)

  defp extract_trace_start_time(%{"startTimeUnixNano" => n}) when is_pos_integer(n),
    do: timestamp_to_microseconds(n)

  defp extract_trace_start_time(_params), do: nil

  defp timestamp_to_microseconds(raw)
       when raw >= 1_000_000_000_000_000_000 and
              raw < 10_000_000_000_000_000_000,
       do: div(raw, 1_000)

  defp timestamp_to_microseconds(raw)
       when raw >= 1_000_000_000_000_000 and
              raw < 10_000_000_000_000_000,
       do: raw

  defp timestamp_to_microseconds(raw)
       when raw >= 1_000_000_000_000 and
              raw < 10_000_000_000_000,
       do: raw * 1_000

  defp timestamp_to_microseconds(raw) when raw >= 1_000_000_000 and raw < 10_000_000_000,
    do: raw * 1_000_000

  defp timestamp_to_microseconds(raw) when raw >= 1_000_000 and raw < 10_000_000,
    do: raw * 1_000_000_000

  defp timestamp_to_microseconds(raw), do: raw

  @spec default_timestamp() :: integer()
  defp default_timestamp do
    System.system_time(:microsecond)
  end
end
