defmodule Logflare.LogEvent do
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Logs.Ingest.MetadataCleaner
  alias Logflare.Source
  alias __MODULE__, as: LE
  alias Logflare.Logs.Validators.{EqDeepFieldTypes, BigQuerySchemaChange}

  require Logger

  @validators [EqDeepFieldTypes, BigQuerySchemaChange]

  @primary_key {:id, :binary_id, []}
  typed_embedded_schema do
    field :body, :map, default: %{}
    embeds_one :source, Source
    field :valid, :boolean
    field :drop, :boolean, default: false
    field :is_from_stale_query, :boolean
    field :validation_error, {:array, :string}
    field :ingested_at, :utc_datetime_usec
    field :sys_uint, :integer
    field :params, :map
    field :origin_source_id, Ecto.UUID.Atom
    field :via_rule, :map
  end

  @doc """
  Used to generate log events from bigquery rows.
  """
  @spec make_from_db(map(), %{source: Source.t()}) :: LE.t()
  def make_from_db(params, %{source: %Source{} = source}) do
    params =
      params
      |> Map.update("metadata", %{}, fn
        [] -> %{}
        [metadata] -> metadata
      end)
      |> mapper(source)

    %__MODULE__{}
    |> cast(params, [:valid, :validation_error, :id, :body])
    |> cast_embed(:source, with: &Source.no_casting_changeset/1)
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
      |> cast(mapper(params, source), [:body, :valid, :validation_error])
      |> cast_embed(:source, with: &Source.no_casting_changeset/1)
      |> validate_required([:body])

    le_map =
      changeset.changes
      |> Map.put(:validation_error, changeset_error_to_string(changeset))
      |> Map.put(:source, source)
      |> Map.put(:origin_source_id, source.token)
      |> Map.put(:valid, changeset.valid?)
      |> Map.put(:params, params)
      |> Map.put(:ingested_at, NaiveDateTime.utc_now())
      |> Map.put(:id, changeset.changes.body["id"])
      |> Map.put(:sys_uint, System.unique_integer([:monotonic]))

    Logflare.LogEvent
    |> struct!(le_map)
    |> validate()
  end

  # Parses input parameters and performs casting.
  defp mapper(params, source) do
    # TODO: deprecate and remove `log_entry` and `message`
    event_message = params["log_entry"] || params["message"] || params["event_message"]
    metadata = params["metadata"]
    id = id(params)

    timestamp =
      case params["timestamp"] do
        x when is_binary(x) ->
          case DateTime.from_iso8601(x) do
            {:ok, udt, _} ->
              DateTime.to_unix(udt, :microsecond)

            {:error, _} ->
              # Logger.warn(
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

        nil ->
          System.system_time(:microsecond)
      end

    body =
      params
      |> Map.merge(%{
        "event_message" => event_message,
        "metadata" => metadata,
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
          {:cont, %{le | valid: true}}

        {:error, message} ->
          {:halt, %{le | valid: false, validation_error: message}}
      end
    end)
  end

  # used to stringify changeset errors
  # TODO: move to utils
  defp changeset_error_to_string(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(value))
      end)
    end)
    |> Enum.reduce("", fn {k, v}, acc ->
      joined_errors = inspect(v)
      "#{acc}#{k}: #{joined_errors}\n"
    end)
  end

  @doc """
  Generates a custom event message from source settings.any()

  The `:custom_event_message_keys` key on the source determines what values are extracted from the log event body and set into the `event_message` key.

  Configuration should be comma separated, and it accepts json query syntax.
  """
  def apply_custom_event_message(%LE{source: %Source{} = source} = le) do
    message = make_message(le, source)

    Kernel.put_in(le.body["event_message"], message)
  end

  defp make_message(le, source) do
    message = le.body["message"] || le.body["event_message"]

    if keys = source.custom_event_message_keys do
      keys
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.map(fn x ->
        case x do
          "id" ->
            le.id

          "message" ->
            message

          "event_message" ->
            message

          "metadata." <> rest ->
            query_json(le.body["metadata"], "$.#{rest}")

          "m." <> rest ->
            query_json(le.body["metadata"], "$.#{rest}")

          keys ->
            ["Invalid custom message keys. Are your keys comma separated? Got: #{inspect(keys)}"]
        end
      end)
      |> Enum.join(" | ")
    else
      message
    end
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
