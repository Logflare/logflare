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
      :insert_protocol,
      :native_port,
      :native_pool_size,
      :use_async_inserts_for_small_batches,
      :async_insert_max_rows,
      :url,
      :read_only_url,
      :async_insert_cluster_url
    ],
    incidentio: [],
    s3: [:s3_bucket, :storage_region, :batch_timeout, :endpoint],
    axiom: [:domain, :dataset_name],
    otlp: [:protocol, :gzip, :endpoint],
    last9: [:region],
    syslog: [:host, :port, :tls, :max_message_bytes]
  }

  @omitted_fields [
    :headers,
    :username,
    :password,
    :api_key,
    :api_token,
    :dsn,
    :access_key_id,
    :secret_access_key,
    :alert_source_config_id,
    :metadata,
    :cipher_key,
    :ca_cert,
    :client_cert,
    :client_key,
    :structured_data
  ]
  @url_fields [:url, :read_only_url, :async_insert_cluster_url, :endpoint]
  @known_fields @safe_fields
                |> Map.values()
                |> List.flatten()
                |> Kernel.++(@omitted_fields)
                |> Enum.uniq()

  @doc false
  @spec safe_fields() :: %{required(atom()) => [atom()]}
  def safe_fields, do: @safe_fields

  @doc false
  @spec known_fields() :: [atom()]
  def known_fields, do: @known_fields

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
    with {:ok, redacted_config} <- redact_config(adaptor, normalize_config(config)) do
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

  defp normalize_config(config) do
    Enum.reduce(@known_fields, %{}, fn key, normalized ->
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
         true <- safe_value?(field, value) do
      Map.put(safe_config, field, value)
    else
      _ -> safe_config
    end
  end

  defp safe_value?(field, value) when field in @url_fields, do: safe_url?(value)
  defp safe_value?(_, value), do: is_binary(value) or is_boolean(value) or is_integer(value)

  defp safe_url?(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host, userinfo: nil, query: nil, fragment: nil, path: path}
      when is_binary(scheme) and is_binary(host) and path in [nil, "", "/"] ->
        true

      _ ->
        false
    end
  end

  defp safe_url?(_), do: false
end
