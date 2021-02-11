defmodule Logflare.LogEvent do
  use TypedEctoSchema
  import Ecto.Changeset
  alias Logflare.Logs.Ingest.MetadataCleaner

  alias Logflare.Source
  alias __MODULE__, as: LE
  alias Logflare.Logs.Validators.{EqDeepFieldTypes, BigQuerySchemaChange}

  use Logflare.ChangefeedSchema

  @validators [EqDeepFieldTypes, BigQuerySchemaChange]

  defmodule Body do
    @moduledoc false
    use TypedEctoSchema

    @primary_key false
    typed_embedded_schema do
      field :metadata, :map, default: %{}
      field :message, :string
      field :timestamp, :integer
      field :created_at, :utc_datetime_usec
    end
  end

  @primary_key {:id, :binary_id, [autogenerate: false]}
  typed_schema "log_events" do
    embeds_one :body, Body
    belongs_to :source, Source
    field :valid, :boolean
    field :is_from_stale_query, :boolean, virtual: true
    field :validation_error, {:array, :string}, virtual: true
    field :ingested_at, :utc_datetime_usec
    field :sys_uint, :integer, virtual: true
    field :params, :map, virtual: true
    field :origin_source_id, Ecto.UUID.Atom, virtual: true
    field :via_rule, :map
    field :ephemeral, :boolean, virtual: true
  end

  def changefeed_changeset(struct, attrs) do
    Logflare.EctoChangesetExtras.cast_all_fields(
      struct,
      attrs
    )
  end

  def mapper(params) do
    message =
      params["custom_message"] || params["log_entry"] || params["message"] ||
        params["event_message"] ||
        params[:event_message]

    metadata = params["metadata"] || params[:metadata]
    id = params["id"] || params[:id]

    timestamp =
      case params["timestamp"] || params[:timestamp] do
        x when is_binary(x) ->
          {:ok, udt, _} = DateTime.from_iso8601(x)
          DateTime.to_unix(udt, :microsecond)

        # FIXME: validate that integer is in appropriate range (and length?)
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

    %{
      "body" => %{
        "message" => message,
        "metadata" => metadata,
        "timestamp" => timestamp
      },
      "id" => id,
      "ephemeral" => params[:ephemeral]
    }
    |> MetadataCleaner.deep_reject_nil_and_empty()
  end

  @spec make_from_db(map(), %{source: Source.t()}) :: LE.t()
  def make_from_db(params, %{source: %Source{} = source}) do
    params =
      params
      |> Map.update(:metadata, %{}, fn
        [] -> %{}
        [metadata] -> metadata
      end)
      |> mapper()

    changes =
      %__MODULE__{}
      |> cast(params, [:valid, :validation_error, :id])
      |> cast_embed(:body, with: &make_body/2)
      |> cast_assoc(:source, with: &Source.no_casting_changeset/1)
      |> Map.get(:changes)

    body = struct!(Body, changes.body.changes)

    __MODULE__
    |> struct!(changes)
    |> Map.put(:body, body)
    |> Map.replace!(:source, source)
  end

  @spec make(%{optional(String.t()) => term}, %{source: Source.t()}) :: LE.t()
  def make(params, %{source: %Source{} = source}) do
    changeset =
      %__MODULE__{}
      |> cast(mapper(params), [:valid, :validation_error, :ephemeral])
      |> put_assoc(:source, source)
      |> cast_embed(:body, with: &make_body/2)
      |> validate_required([:body])

    body = struct!(Body, changeset.changes.body.changes)

    le_map =
      changeset.changes
      |> Map.put(:body, body)
      |> Map.put(:validation_error, changeset_error_to_string(changeset))
      |> Map.put(:source, source)
      |> Map.put(:source_id, source.id)
      |> Map.put(:origin_source_id, source.token)
      |> Map.put(:valid, changeset.valid?)
      |> Map.put(:params, params)
      |> Map.put(:ingested_at, DateTime.utc_now())
      |> Map.put(:id, Ecto.UUID.generate())
      |> Map.put(:sys_uint, System.unique_integer([:monotonic]))

    Logflare.LogEvent
    |> struct!(le_map)
    |> validate()
  end

  def make_body(_struct, params) do
    %__MODULE__.Body{}
    |> cast(params, [
      :metadata,
      :message,
      :timestamp
    ])
    |> validate_required([:message, :timestamp])
    |> validate_length(:message, min: 1)
  end

  @spec validate(LE.t()) :: LE.t()
  def validate(%LE{valid: false} = le), do: le

  def validate(%LE{valid: true} = le) do
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

  def changeset_error_to_string(changeset) do
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
end
