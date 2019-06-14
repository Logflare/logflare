defmodule Logflare.LogEvent do
  use Ecto.Schema
  import Ecto.Changeset
  alias Logflare.Logs.Injest.MetadataCleaner
  alias __MODULE__, as: LE
  alias Logflare.Logs.Validators.{EqDeepFieldTypes, BigQuerySchemaSpec}

  @validators [EqDeepFieldTypes, BigQuerySchemaSpec]

  defmodule Body do
    @moduledoc false
    use Ecto.Schema

    @primary_key false
    embedded_schema do
      field :metadata, :map, default: %{}
      field :message, :string
      field :timestamp, :integer
    end
  end

  @primary_key false
  embedded_schema do
    embeds_one :body, Body
    field :source, :map
    field :valid?, :boolean
    field :validation_error, {:array, :string}
  end

  def mapper(params) do
    message = params["log_entry"] || params["message"]
    metadata = params["metadata"]

    timestamp =
      case params["timestamp"] do
        x when is_binary(x) ->
          unix =
            x
            |> Timex.parse!("{ISO:Extended}")
            |> Timex.to_unix()

          unix * 1_000_000

        x when is_integer(x) ->
          x

        nil ->
          System.system_time(:microsecond)
      end

    %{
      "body" => %{
        "message" => message,
        "metadata" => metadata,
        "timestamp" => timestamp
      }
    }
    |> MetadataCleaner.deep_reject_nil_and_empty()
  end

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
    |> Map.put(:valid?, changeset.valid?)
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
