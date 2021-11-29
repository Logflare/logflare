defmodule Logflare.LogEvent do
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Logs.Ingest.MetadataCleaner
  alias Logflare.Source
  alias __MODULE__, as: LE
  alias Logflare.Logs.Validators.{EqDeepFieldTypes, BigQuerySchemaChange}

  require Logger

  @validators [EqDeepFieldTypes, BigQuerySchemaChange]

  defmodule Body do
    @moduledoc false
    use TypedEctoSchema

    @primary_key false
    typed_embedded_schema do
      field :id, Ecto.UUID.Atom
      field :metadata, :map, default: %{}
      field :message, :string
      field :timestamp, :integer
      # dev hack
      field :level, :string
      # prod hack
      field :project, :string
    end
  end

  @primary_key {:id, :binary_id, []}
  typed_embedded_schema do
    embeds_one :body, Body
    embeds_one :source, Source
    field :valid, :boolean
    field :is_from_stale_query, :boolean
    field :validation_error, {:array, :string}
    field :ingested_at, :utc_datetime_usec
    field :sys_uint, :integer
    field :params, :map
    field :origin_source_id, Ecto.UUID.Atom
    field :via_rule, :map
    field :ephemeral, :boolean
    field :make_from, :string
  end

  def mapper(params, source) do
    message =
      params["log_entry"] || params["message"] ||
        params["event_message"] ||
        params[:event_message]

    metadata = params["metadata"] || params[:metadata]

    id = id(params)

    timestamp =
      case params["timestamp"] || params[:timestamp] do
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

    body =
      %{
        "message" => message,
        "metadata" => metadata,
        "timestamp" => timestamp,
        "id" => id
      }
      |> put_clustering_keys(source)

    %{
      "body" => body,
      "id" => id,
      "ephemeral" => params[:ephemeral],
      "make_from" => params[:make_from]
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
      |> Map.put(:make_from, "db")
      |> mapper(source)

    changes =
      %__MODULE__{}
      |> cast(params, [:valid, :validation_error, :id, :make_from])
      |> cast_embed(:body, with: &make_body/2)
      |> cast_embed(:source, with: &Source.no_casting_changeset/1)
      |> Map.get(:changes)

    body = struct!(Body, changes.body.changes)

    __MODULE__
    |> struct!(changes)
    |> Map.put(:body, body)
    |> Map.replace!(:source, source)
  end

  @spec make(%{optional(String.t()) => term}, %{source: Source.t()}) :: LE.t()
  def make(params, %{source: source}) do
    changeset =
      %__MODULE__{}
      |> cast(mapper(params, source), [:valid, :validation_error, :ephemeral, :make_from])
      |> cast_embed(:source, with: &Source.no_casting_changeset/1)
      |> cast_embed(:body, with: &make_body/2)
      |> validate_required([:body])

    body = struct!(Body, changeset.changes.body.changes)

    le_map =
      changeset.changes
      |> Map.put(:body, body)
      |> Map.put(:validation_error, changeset_error_to_string(changeset))
      |> Map.put(:source, source)
      |> Map.put(:origin_source_id, source.token)
      |> Map.put(:valid, changeset.valid?)
      |> Map.put(:params, params)
      |> Map.put(:ingested_at, NaiveDateTime.utc_now())
      |> Map.put(:id, body.id)
      |> Map.put(:sys_uint, System.unique_integer([:monotonic]))

    Logflare.LogEvent
    |> struct!(le_map)
    |> validate()
  end

  def make_body(_struct, params) do
    %__MODULE__.Body{}
    |> cast(params, [
      :id,
      :metadata,
      :message,
      :timestamp,
      :level
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

  def put_clustering_keys(params, source) do
    case source.token do
      # dev
      :"83e59828-b6ee-408c-92a8-c17bc523e6e0" ->
        key = Kernel.get_in(params, ["metadata", "level"])

        params |> Map.put("level", key)

      # prod Postgres logs
      :"74c7911a-4671-46b7-9c7f-440a18bc6bad" ->
        key = Kernel.get_in(params, ["metadata", "project"])

        params |> Map.put("project", key)

      # prod Cloudflare
      :"7b5df630-a551-4c79-ae17-042650b37a3e" ->
        host = Kernel.get_in(params, ["metadata", "request", "host"])
        project = String.split(host, ".") |> Enum.at(0)

        params |> Map.put("project", project)

      _ ->
        params
    end
  end

  def apply_custom_event_message(%LE{source: %Source{} = source} = le) do
    message = make_message(le, source)

    Kernel.put_in(le.body.message, message)
  end

  defp make_message(le, source) do
    message = le.body.message

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

          "metadata." <> rest ->
            query_json(le.body.metadata, "$.#{rest}")

          "m." <> rest ->
            query_json(le.body.metadata, "$.#{rest}")

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
