defmodule Logflare.LogEvent do
  use TypedEctoSchema

  import Ecto.Changeset
  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1]

  alias Logflare.Logs.Ingest.MetadataCleaner
  alias Logflare.Sources.Source
  alias __MODULE__, as: LE
  alias Logflare.Logs.Validators.{EqDeepFieldTypes, BigQuerySchemaChange}
  alias Logflare.Logs.IngestTransformers

  require Logger

  @validators [EqDeepFieldTypes, BigQuerySchemaChange]

  @primary_key {:id, :binary_id, []}
  typed_embedded_schema do
    field :body, :map, default: %{}
    field :valid, :boolean
    field :drop, :boolean, default: false
    field :is_from_stale_query, :boolean
    field :ingested_at, :utc_datetime_usec
    field :sys_uint, :integer
    field :params, :map
    field :origin_source_id, Ecto.UUID.Atom
    field :via_rule, :map

    embeds_one :source, Source

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
    |> cast_embed(:source, with: &Source.no_casting_changeset/1)
    |> cast_embed(:pipeline_error, with: &pipeline_error_changeset/2)
    |> apply_changes()
    |> Map.put(:source, source)
  end

  @doc """
  Used to make log event from user-provided parameters, for ingestion.
  """
  @spec make(%{optional(String.t()) => term}, %{source: Source.t()}) :: LE.t()
  def make(params, %{source: source}) do
    changeset =
      %__MODULE__{}
      |> cast(mapper(params), [:body, :valid])
      |> cast_embed(:source, with: &Source.no_casting_changeset/1)
      |> cast_embed(:pipeline_error, with: &pipeline_error_changeset/2)
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
      changeset.changes
      |> Map.put(:pipeline_error, pipeline_error)
      |> Map.put(:source, source)
      |> Map.put(:origin_source_id, source.token)
      |> Map.put(:valid, changeset.valid?)
      |> Map.put(:params, params)
      |> Map.put(:ingested_at, NaiveDateTime.utc_now())
      |> Map.put(:id, changeset.changes.body["id"])
      |> Map.put(:sys_uint, System.unique_integer([:monotonic]))

    Logflare.LogEvent
    |> struct!(le_map)
    |> transform()
    |> validate()
  end

  # Parses input parameters and performs casting.
  defp mapper(params) do
    # TODO: deprecate and remove `log_entry` and `message`
    event_message = params["log_entry"] || params["message"] || params["event_message"]
    id = id(params)

    timestamp =
      case params["timestamp"] do
        x when is_binary(x) ->
          case DateTime.from_iso8601(x) do
            {:ok, udt, _} ->
              DateTime.to_unix(udt, :microsecond)

            {:error, _} ->
              # Logger.warning(
              #   "Malformed timesetamp. Using DateTime.utc_now/0. Expected iso8601. Got: #{inspect(x)}"
              # )

              System.system_time(:microsecond)
          end

        x when is_integer(x) ->
          case Integer.digits(x) |> Enum.count() do
            19 -> Kernel.round(x / 1_000)
            16 -> x
            13 -> x * 1_000
            10 -> x * 1_000_000
            7 -> x * 1_000_000_000
            _ -> x
          end

        x when is_float(x) ->
          rounded = round(x)
          length = rounded |> Integer.digits() |> Enum.count()

          case length do
            19 -> round(rounded / 1_000)
            16 -> rounded
            13 -> x * 1_000
            10 -> x * 1_000_000
            7 -> x * 1_000_000_000
            _ -> rounded
          end

        nil ->
          System.system_time(:microsecond)
      end

    body =
      params
      |> Map.merge(%{
        "event_message" => event_message,
        "timestamp" => timestamp,
        "id" => id
      })
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
    |> MetadataCleaner.deep_reject_nil_and_empty()
  end

  @spec validate(LE.t()) :: LE.t()
  defp validate(%LE{valid: false} = le), do: le

  defp validate(%LE{valid: true} = le) do
    @validators
    |> Enum.reduce_while(true, fn validator, _acc ->
      case validator.validate(le) do
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

  @spec transform(LE.t()) :: LE.t()
  defp transform(%LE{valid: false} = le), do: le

  defp transform(%LE{valid: true} = le) do
    with {:ok, le} <- bigquery_spec(le),
         {:ok, le} <- copy_fields(le) do
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

  defp copy_fields(%LE{source: %Source{transform_copy_fields: nil}} = le), do: {:ok, le}

  defp copy_fields(le) do
    instructions = String.split(le.source.transform_copy_fields, ~r/\n/, trim: true)

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

  @doc """
  Generates a custom event message from source settings.any()

  The `:custom_event_message_keys` key on the source determines what values are extracted from the log event body and set into the `event_message` key.

  Configuration should be comma separated, and it accepts json query syntax.
  """
  def apply_custom_event_message(%LE{drop: true} = le), do: le

  def apply_custom_event_message(%LE{source: %Source{} = source} = le) do
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
        log_event.id

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
        inspect(v)

      {:error, _} ->
        "json_path_query_error"
    end
  end

  defp id(params) do
    params["id"] || params[:id] || Ecto.UUID.generate()
  end
end
