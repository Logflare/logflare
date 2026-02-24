defmodule Logflare.LogEvent do
  use TypedEctoSchema

  import Ecto.Changeset
  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1]

  alias __MODULE__, as: LE
  alias __MODULE__.TypeDetection
  alias Logflare.Logs.Ingest.MetadataCleaner
  alias Logflare.Logs.IngestTransformers
  alias Logflare.Logs.Validators.BigQuerySchemaChange
  alias Logflare.KeyValues
  alias Logflare.Sources.Source

  require Logger

  @validators [BigQuerySchemaChange]

  @primary_key {:id, :binary_id, []}
  typed_embedded_schema do
    field :body, :map, default: %{}
    field :valid, :boolean
    field :drop, :boolean, default: false
    field :is_from_stale_query, :boolean
    field :ingested_at, :utc_datetime_usec
    field :source_uuid, Ecto.UUID.Atom
    field :source_name, :string
    field :via_rule, :map
    field :retries, :integer, default: 0
    field :event_type, Ecto.Enum, values: [:log, :metric, :trace], default: :log

    field :source_id, :integer, default: nil

    embeds_one :pipeline_error, PipelineError do
      field :stage, :string
      field :type, :string
      field :message, :string
    end
  end

  @doc """
  Used to generate log events from bigquery rows.
  """
  @spec make_from_db(map(), %{source: Source.t()}) :: LE.t()
  def make_from_db(params, %{source: %Source{} = source}) do
    params =
      params
      |> mapper()

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
  def make(params, %{source: source}, _opts \\ []) do
    changeset =
      %__MODULE__{}
      |> cast(mapper(params), [:body, :valid])
      |> validate_required([:body])

    pipeline_error =
      if changeset.valid?,
        do: nil,
        else: %LE.PipelineError{
          stage: "changeset",
          type: "validators",
          message: stringify_changeset_errors(changeset)
        }

    le_map =
      Map.merge(changeset.changes, %{
        pipeline_error: pipeline_error,
        source_id: source.id,
        source_uuid: source.token,
        source_name: source.name,
        valid: changeset.valid?,
        ingested_at: NaiveDateTime.utc_now(),
        id: changeset.changes.body["id"],
        event_type: TypeDetection.detect(params)
      })

    Logflare.LogEvent
    |> struct!(le_map)
    |> transform(source)
    |> validate(source)
  end

  # Parses input parameters and performs casting.
  defp mapper(params) do
    # TODO: deprecate and remove `message`
    event_message = params["message"] || params["event_message"]
    id = id(params)

    timestamp = determine_timestamp(params)

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
      |> MetadataCleaner.deep_reject_nil_and_empty()
      |> Map.merge(base_merge)
      |> case do
        %{"message" => m, "event_message" => em} = map when m == em ->
          Map.delete(map, "message")

        other ->
          other
      end

    %{
      "body" => body,
      "id" => id
    }
  end

  @spec validate(LE.t(), Source.t()) :: LE.t()
  defp validate(%LE{valid: false} = le, _source), do: le

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

  @spec transform(LE.t(), Source.t()) :: LE.t()
  defp transform(%LE{valid: false} = le, _source), do: le

  defp transform(%LE{valid: true} = le, %Source{} = source) do
    with {:ok, le} <- bigquery_spec(le),
         {:ok, le} <- copy_fields(le, source),
         {:ok, le} <- kv_enrich(le, source) do
      le
    else
      {:error, message} ->
        %{
          le
          | valid: false,
            pipeline_error: %LE.PipelineError{
              stage: "transform",
              type: "transform",
              message: message
            }
        }
    end
  end

  defp bigquery_spec(le) do
    new_body = IngestTransformers.transform(le.body, :to_bigquery_column_spec)
    {:ok, %{le | body: new_body}}
  end

  defp copy_fields(%LE{} = le, %Source{transform_copy_fields: nil}), do: {:ok, le}

  defp copy_fields(le, source) do
    instructions = String.split(source.transform_copy_fields, ~r/\n/, trim: true)

    new_body =
      for instruction <- instructions, instruction = String.trim(instruction), reduce: le.body do
        acc ->
          case String.split(instruction, ":", parts: 2) do
            [from, to] ->
              from = String.replace_prefix(from, "m.", "metadata.")
              from_path = String.split(from, ".")

              to = String.replace_prefix(to, "m.", "metadata.")
              to_path = String.split(to, ".")

              if value = get_in(acc, from_path) do
                put_in(acc, Enum.map(to_path, &Access.key(&1, %{})), value)
              else
                acc
              end

            _ ->
              acc
          end
      end

    {:ok, Map.put(le, :body, new_body)}
  end

  defp kv_enrich(%LE{} = le, %Source{transform_key_values: nil, transform_key_values_parsed: nil}),
       do: {:ok, le}

  defp kv_enrich(%LE{} = le, %Source{transform_key_values_parsed: []}), do: {:ok, le}

  defp kv_enrich(%LE{} = le, %Source{transform_key_values_parsed: parsed, user_id: user_id})
       when is_list(parsed) do
    new_body =
      Enum.reduce(parsed, le.body, fn instruction, acc ->
        apply_kv_instruction(acc, instruction, user_id)
      end)

    {:ok, Map.put(le, :body, new_body)}
  end

  # Fallback: parse at ingestion time when parsed field is not populated
  defp kv_enrich(%LE{} = le, %Source{} = source) do
    kv_enrich(le, Source.parse_key_values_config(source))
  end

  defp apply_kv_instruction(
         body,
         %{from_path: from_path, to_path: to_path} = instruction,
         user_id
       ) do
    accessor_path = Map.get(instruction, :accessor_path)

    with raw when not is_nil(raw) <- get_in(body, from_path),
         raw_string <- to_string(raw),
         true <- Logflare.Utils.flag("key_values", raw_string),
         value when not is_nil(value) <-
           KeyValues.Cache.lookup(user_id, raw_string, accessor_path) do
      put_in(body, Enum.map(to_path, &Access.key(&1, %{})), value)
    else
      _ -> body
    end
  end

  @doc """
  Generates a custom event message from source settings.any()

  The `:custom_event_message_keys` key on the source determines what values are extracted from the log event body and set into the `event_message` key.

  Configuration should be comma separated, and it accepts json query syntax.
  """
  def apply_custom_event_message(%LE{drop: true} = le, _source), do: le

  def apply_custom_event_message(%LE{} = le, %Source{} = source) do
    message = make_message(le, source)

    Kernel.put_in(le.body["event_message"], message)
  end

  @doc """
  Changeset for pipeline errors.
  """
  def pipeline_error_changeset(pipeline_error, attrs) do
    pipeline_error
    |> cast(attrs, [
      :stage,
      :message
    ])
    |> validate_required([:stage, :message])
  end

  defp make_message(log_event, source) do
    if keys = source.custom_event_message_keys do
      keys
      |> String.split(",", trim: true)
      |> Enum.map_join(" | ", fn key -> build_message(key, log_event) end)
    else
      get_default_message(log_event)
    end
  end

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

  defp get_default_message(log_event) do
    log_event.body["message"] || log_event.body["event_message"]
  end

  defp query_json(metadata, query) do
    case Warpath.query(metadata, query) do
      {:ok, v} ->
        Jason.encode!(v)

      {:error, _} ->
        "json_path_query_error"
    end
  end

  defp id(params) do
    params["id"] || params[:id] || Ecto.UUID.generate()
  end

  defp determine_timestamp(params) when not is_map_key(params, "timestamp"),
    do: default_timestamp()

  defp determine_timestamp(%{"timestamp" => x}) when is_binary(x) do
    case DateTime.from_iso8601(x) do
      {:ok, udt, _} ->
        DateTime.to_unix(udt, :microsecond)

      {:error, _} ->
        default_timestamp()
    end
  end

  defp determine_timestamp(%{"timestamp" => x}) when is_integer(x) do
    Logflare.Utils.to_microseconds(x)
  end

  defp determine_timestamp(%{"timestamp" => x}) when is_float(x) do
    determine_timestamp(%{"timestamp" => round(x)})
  end

  defp determine_timestamp(_), do: default_timestamp()

  defp default_timestamp() do
    System.system_time(:microsecond)
  end
end
