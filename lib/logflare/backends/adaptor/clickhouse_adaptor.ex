defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor do
  @moduledoc """
  ClickHouse backend adaptor

  ### Table Creation

  Currently this adaptor expects a table to be created with the following schema.

  Adjust the database / table names as needed.

  ```sql
  CREATE TABLE "default"."supabase_log_ingress" (
        "id" UUID,
        "event_message" String,
        "body" String,
        "timestamp" DateTime64(6)
    )
    ENGINE MergeTree()
    ORDER BY ("timestamp")
    SETTINGS index_granularity = 8192 SETTINGS flatten_nested=0
  ```
  """

  use TypedStruct

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor.WebhookAdaptor

  typedstruct enforce: true do
    field(:url, String.t())
    field(:url_override, String.t(), default: nil)
    field(:username, String.t())
    field(:password, String.t())
    field(:database, String.t(), default: "default")
    field(:table, String.t(), default: "supabase_log_ingress")
    field(:port, non_neg_integer(), default: 8443)
  end

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend.config)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(config) do
    headers =
      %{
        "X-ClickHouse-Database" => config.database,
        "X-ClickHouse-User" => config.username,
        "X-ClickHouse-Key" => config.password
      }

    uri =
      case URI.new(config.url) do
        {:ok, %URI{} = uri_temp} ->
          %URI{uri_temp | port: config.port}

        _ ->
          raise "Unable to parse Clickhouse URL: '#{inspect(config.url)}'"
      end

    # Generate the insert query we'll append to the URL
    insert_query =
      URI.encode_query(
        %{
          "query" => "INSERT INTO #{config.table} FORMAT JSONEachRow"
        },
        :rfc3986
      )

    updated_url =
      URI.append_query(uri, insert_query)
      |> URI.to_string()

    %{
      url: updated_url,
      url_override: updated_url,
      database: config.database,
      table: config.table,
      port: config.port,
      headers: headers,
      http: "http2",
      gzip: true
    }
  end

  @impl Logflare.Backends.Adaptor
  def pre_ingest(_source, _backend, log_events) do
    Enum.map(log_events, &translate_event/1)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{},
     %{
       url: :string,
       username: :string,
       password: :string,
       database: :string,
       table: :string,
       port: :integer
     }}
    |> Changeset.cast(params, [
      :url,
      :username,
      :password,
      :database,
      :table,
      :port
    ])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url, :database, :table, :port])
    |> Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_user_pass()
  end

  defp validate_user_pass(changeset) do
    user = Changeset.get_field(changeset, :username)
    pass = Changeset.get_field(changeset, :password)
    user_pass = [user, pass]

    if user_pass != [nil, nil] and Enum.any?(user_pass, &is_nil/1) do
      msg = "Both username and password must be provided for auth"

      changeset
      |> Changeset.add_error(:username, msg)
      |> Changeset.add_error(:password, msg)
    else
      changeset
    end
  end

  defp translate_event(%Logflare.LogEvent{body: body} = le) do
    %Logflare.LogEvent{
      le
      | body: %{
          "id" => le.id,
          "event_message" => body["event_message"] || "",
          "body" => Map.drop(body, ["id", "event_message", "timestamp"]),
          "timestamp" => body["timestamp"]
        }
    }
  end
end
