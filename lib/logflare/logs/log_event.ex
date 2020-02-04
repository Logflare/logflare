defmodule Logflare.LogEvent do
  use Ecto.Schema
  import Ecto.Changeset
  alias Logflare.Logs.Ingest.MetadataCleaner
  alias Logflare.Source
  alias __MODULE__, as: LE
  alias Logflare.Logs.Validators.{EqDeepFieldTypes, BigQuerySchemaSpec, BigQuerySchemaChange}

  @validators [EqDeepFieldTypes, BigQuerySchemaSpec, BigQuerySchemaChange]

  defmodule Body do
    @moduledoc false
    use Ecto.Schema

    @primary_key false
    embedded_schema do
      field :metadata, :map, default: %{}
      field :message, :string
      field :timestamp, :integer
      field :created_at, :utc_datetime_usec
    end

    @type t() :: %__MODULE__{
            metadata: map(),
            message: String.t(),
            timestamp: non_neg_integer(),
            created_at: DateTime.t()
          }
  end

  @primary_key {:id, :binary_id, []}
  embedded_schema do
    embeds_one :body, Body
    field :source, :map
    field :valid?, :boolean
    field :validation_error, {:array, :string}
    field :ingested_at, :utc_datetime_usec
    field :sys_uint, :integer
    field :params, :map
    field :origin_source_id, Ecto.UUID.Atom
    field :via_rule, :map
  end

  @type t() :: %__MODULE__{
          valid?: boolean(),
          validation_error: [String.t()],
          ingested_at: DateTime.t(),
          sys_uint: integer(),
          params: map(),
          body: Body.t()
        }

  def mapper(params) do
    message = params["log_entry"] || params["message"] || params["event_message"]
    metadata = params["metadata"] || params
    id = params["id"]

    timestamp =
      case params["timestamp"] do
        x when is_binary(x) ->
          {:ok, udt, 0} = DateTime.from_iso8601(x)
          DateTime.to_unix(udt, :microsecond)

        # FIXME: validate that integer is in appropriate range (and length?)
        x when is_integer(x) ->
          case Integer.digits(x) |> Enum.count() do
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
      "id" => id
    }
    |> MetadataCleaner.deep_reject_nil_and_empty()
  end

  def make_from_db(params, %{source: _source}) do
    params =
      params
      |> Map.update("metadata", %{}, fn metadata ->
        if metadata == [], do: %{}, else: hd(metadata)
      end)
      |> mapper()

    changes =
      %__MODULE__{}
      |> cast(params, [:source, :valid?, :validation_error, :id])
      |> cast_embed(:body, with: &make_body/2)
      |> Map.get(:changes)

    body = struct!(Body, changes.body.changes)

    __MODULE__
    |> struct!(changes)
    |> Map.put(:body, body)
  end

  @spec make(%{optional(String.t()) => term}, %{source: Source.t()}) :: LE.t()
  def make(params, %{source: source}) do
    changeset =
      %__MODULE__{}
      |> cast(mapper(params), [:source, :valid?, :validation_error])
      |> cast_embed(:body, with: &make_body/2)
      |> validate_required([:body])

    body = struct!(Body, changeset.changes.body.changes)

    __MODULE__
    |> struct!(changeset.changes)
    |> Map.put(:body, body)
    |> Map.put(:validation_error, changeset_error_to_string(changeset))
    |> Map.put(:source, source)
    |> Map.put(:origin_source_id, source.token)
    |> Map.put(:valid?, changeset.valid?)
    |> Map.put(:params, params)
    |> Map.put(:ingested_at, NaiveDateTime.utc_now())
    |> Map.put(:id, Ecto.UUID.generate())
    |> Map.put(:sys_uint, System.unique_integer([:monotonic]))
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
  def validate(%LE{valid?: false} = le), do: le

  def validate(%LE{valid?: true} = le) do
    @validators
    |> Enum.reduce_while(true, fn validator, _acc ->
      case validator.validate(le) do
        :ok ->
          {:cont, %{le | valid?: true}}

        {:error, message} ->
          {:halt, %{le | valid?: false, validation_error: message}}
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
