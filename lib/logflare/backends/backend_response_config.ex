defmodule Logflare.Backends.BackendResponseConfig do
  @moduledoc false

  alias Logflare.Backends.Backend

  @safe_fields %{
    webhook: [:url, :http, :gzip],
    elastic: [:url],
    datadog: [:region],
    sentry: [],
    postgres: [:hostname, :database, :schema, :port, :pool_size, :url],
    bigquery: [:project_id, :dataset_id],
    loki: [:url],
    clickhouse: [
      :database,
      :port,
      :pool_size,
      :use_async_inserts_for_small_batches,
      :async_insert_max_rows,
      :max_event_age_hours,
      :url,
      :read_only_url,
      :read_only_urls,
      :default_read_cluster,
      :async_insert_cluster_url
    ],
    incidentio: [],
    s3: [:s3_bucket, :storage_region, :batch_timeout, :endpoint],
    axiom: [:domain, :dataset_name],
    otlp: [:protocol, :gzip, :flatten_to_attributes, :endpoint],
    last9: [:region],
    syslog: [:host, :port, :tls, :max_message_bytes]
  }

  @omitted_fields %{
    webhook: [:headers],
    elastic: [:username, :password],
    datadog: [:api_key],
    sentry: [:dsn],
    postgres: [:username, :password],
    bigquery: [],
    loki: [:headers, :username, :password],
    clickhouse: [:username, :password],
    incidentio: [:api_token, :alert_source_config_id, :metadata],
    s3: [:access_key_id, :secret_access_key],
    axiom: [:api_token],
    otlp: [:headers],
    last9: [:username, :password],
    syslog: [:cipher_key, :ca_cert, :client_cert, :client_key, :structured_data]
  }

  @url_fields [:url, :read_only_url, :async_insert_cluster_url, :endpoint]
  @url_map_fields [:read_only_urls]
  @safe_url_schemes ["http", "https", "postgres", "postgresql"]
  @known_fields Map.new(@safe_fields, fn {type, safe_fields} ->
                  {type, Enum.uniq(safe_fields ++ Map.fetch!(@omitted_fields, type))}
                end)

  @doc false
  @spec safe_fields() :: %{required(atom()) => [atom()]}
  def safe_fields, do: @safe_fields

  @doc false
  @spec omitted_fields() :: %{required(atom()) => [atom()]}
  def omitted_fields, do: @omitted_fields

  @doc false
  @spec known_fields() :: [atom()]
  def known_fields do
    @known_fields
    |> Map.values()
    |> List.flatten()
    |> Enum.uniq()
  end

  @doc false
  @spec known_fields(atom()) :: [atom()]
  def known_fields(type), do: Map.get(@known_fields, type, [])

  @spec serialize(atom(), map() | term()) :: map()
  def serialize(type, config) when is_atom(type) and is_map(config) do
    with {:ok, adaptor} <- fetch_adaptor(type) do
      serialize_with_adaptor(type, adaptor, config)
    else
      _ -> %{}
    end
  end

  def serialize(_, _), do: %{}

  @doc false
  @spec serialize_with_adaptor(atom(), module(), map() | term()) :: map()
  def serialize_with_adaptor(type, adaptor, config)
      when is_atom(type) and is_atom(adaptor) and is_map(config) do
    with {:ok, redacted_config} <- redact_config(adaptor, normalize_config(type, config)) do
      retain_safe_fields(type, redacted_config)
    else
      _ -> %{}
    end
  end

  def serialize_with_adaptor(_, _, _), do: %{}

  defp fetch_adaptor(type) do
    with adaptor when is_atom(adaptor) <- Backend.adaptor_mapping()[type],
         {:module, ^adaptor} <- Code.ensure_loaded(adaptor),
         true <- function_exported?(adaptor, :redact_config, 1) do
      {:ok, adaptor}
    else
      _ -> :error
    end
  end

  defp redact_config(adaptor, config) do
    case adaptor.redact_config(config) do
      %{} = redacted_config -> {:ok, redacted_config}
      _ -> :error
    end
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp normalize_config(type, config) do
    Enum.reduce(Map.get(@known_fields, type, []), %{}, fn key, normalized ->
      case fetch_config_value(config, key) do
        {:ok, value} -> Map.put(normalized, key, value)
        :error -> normalized
      end
    end)
  end

  defp fetch_config_value(config, key) do
    case Map.fetch(config, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(config, Atom.to_string(key))
    end
  end

  defp retain_safe_fields(type, config) do
    @safe_fields
    |> Map.get(type, [])
    |> Enum.reduce(%{}, &retain_safe_field(config, &1, &2))
  end

  defp retain_safe_field(config, field, safe_config) do
    with {:ok, value} <- Map.fetch(config, field),
         {:ok, safe_value} <- safe_value(field, value) do
      Map.put(safe_config, field, safe_value)
    else
      _ -> safe_config
    end
  end

  defp safe_value(field, value) when field in @url_fields do
    if safe_url?(value), do: {:ok, value}, else: :error
  end

  defp safe_value(field, value) when field in @url_map_fields and is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn
      {label, url}, {:ok, safe_urls} when is_binary(label) ->
        if safe_url?(url) do
          {:cont, {:ok, Map.put(safe_urls, label, url)}}
        else
          {:halt, :error}
        end

      _, _acc ->
        {:halt, :error}
    end)
  end

  defp safe_value(_, value) when is_binary(value) or is_boolean(value) or is_integer(value),
    do: {:ok, value}

  defp safe_value(_, _), do: :error

  defp safe_url?(url) when is_binary(url) do
    with {:ok,
          %URI{
            scheme: scheme,
            host: host,
            userinfo: nil,
            query: nil,
            fragment: nil,
            path: path
          }} <- URI.new(url),
         true <- scheme in @safe_url_schemes,
         true <- safe_host?(host),
         true <- path in [nil, "", "/"] do
      true
    else
      _ -> false
    end
  end

  defp safe_url?(_), do: false

  defp safe_host?(host) when is_binary(host) and host != "" do
    String.printable?(host) and
      not String.contains?(host, ["%", "\\"]) and
      not Regex.match?(~r/\s/u, host)
  end

  defp safe_host?(_), do: false
end
