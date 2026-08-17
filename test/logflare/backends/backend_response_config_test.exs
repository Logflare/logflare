defmodule Logflare.Backends.BackendResponseConfigTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendResponseConfig

  defmodule RaisingRedactor do
    def redact_config(_config), do: raise("synthetic redactor failure")
  end

  defmodule ThrowingRedactor do
    def redact_config(_config), do: throw(:synthetic_redactor_failure)
  end

  defmodule NonMapRedactor do
    def redact_config(_config), do: :not_a_map
  end

  defmodule MissingRedactor do
  end

  @configs %{
    webhook: %{
      url: "https://example.com",
      http: "http2",
      gzip: true,
      headers: %{"X-Key" => "synthetic-secret"}
    },
    elastic: %{
      url: "https://example.com",
      username: "synthetic-user",
      password: "synthetic-secret"
    },
    datadog: %{region: "US1", api_key: "synthetic-secret"},
    sentry: %{dsn: "https://key:synthetic-secret@example.com/1"},
    postgres: %{
      hostname: "db.example.com",
      database: "logs",
      schema: "public",
      port: 5432,
      pool_size: 2,
      username: "synthetic-user",
      password: "synthetic-secret"
    },
    bigquery: %{project_id: "project-id", dataset_id: "dataset", arbitrary: "synthetic-secret"},
    loki: %{
      url: "https://example.com",
      username: "synthetic-user",
      password: "synthetic-secret",
      headers: %{"X-Key" => "synthetic-secret"}
    },
    clickhouse: %{
      url: "https://example.com",
      database: "default",
      port: 8123,
      pool_size: 2,
      username: "synthetic-user",
      password: "synthetic-secret",
      read_only_url: "https://legacy-read.example.com",
      read_only_urls: %{
        "primary" => "https://read.example.com",
        "secondary" => "https://read-2.example.com"
      },
      default_read_cluster: "primary",
      use_async_inserts_for_small_batches: false,
      async_insert_cluster_url: "https://async.example.com",
      async_insert_max_rows: 1000,
      max_event_age_hours: 72
    },
    incidentio: %{
      api_token: "synthetic-secret",
      alert_source_config_id: "source",
      metadata: %{secret: "synthetic-secret"}
    },
    s3: %{
      s3_bucket: "bucket",
      storage_region: "us-east-1",
      batch_timeout: 1000,
      endpoint: "https://s3.example.com",
      access_key_id: "synthetic-user",
      secret_access_key: "synthetic-secret"
    },
    axiom: %{domain: "api.axiom.co", dataset_name: "logs", api_token: "synthetic-secret"},
    otlp: %{
      endpoint: "https://example.com",
      protocol: "http/protobuf",
      gzip: true,
      flatten_to_attributes: true,
      headers: %{"X-Key" => "synthetic-secret"}
    },
    last9: %{region: "US-WEST-1", username: "synthetic-user", password: "synthetic-secret"},
    syslog: %{
      host: "syslog.example.com",
      port: 6514,
      tls: true,
      structured_data: "[credential@1 token=\"synthetic-secret\"]",
      max_message_bytes: 1000,
      cipher_key: "synthetic-secret",
      ca_cert: "synthetic-secret",
      client_cert: "synthetic-secret",
      client_key: "synthetic-secret"
    }
  }

  @expected_response_configs %{
    webhook: %{url: "https://example.com", http: "http2", gzip: true},
    elastic: %{url: "https://example.com"},
    datadog: %{region: "US1"},
    sentry: %{},
    postgres: %{
      hostname: "db.example.com",
      database: "logs",
      schema: "public",
      port: 5432,
      pool_size: 2
    },
    bigquery: %{project_id: "project-id", dataset_id: "dataset"},
    loki: %{url: "https://example.com"},
    clickhouse: %{
      url: "https://example.com",
      database: "default",
      port: 8123,
      pool_size: 2,
      read_only_url: "https://legacy-read.example.com",
      read_only_urls: %{
        "primary" => "https://read.example.com",
        "secondary" => "https://read-2.example.com"
      },
      default_read_cluster: "primary",
      use_async_inserts_for_small_batches: false,
      async_insert_cluster_url: "https://async.example.com",
      async_insert_max_rows: 1000,
      max_event_age_hours: 72
    },
    incidentio: %{},
    s3: %{
      s3_bucket: "bucket",
      storage_region: "us-east-1",
      batch_timeout: 1000,
      endpoint: "https://s3.example.com"
    },
    axiom: %{domain: "api.axiom.co", dataset_name: "logs"},
    otlp: %{
      endpoint: "https://example.com",
      protocol: "http/protobuf",
      gzip: true,
      flatten_to_attributes: true
    },
    last9: %{region: "US-WEST-1"},
    syslog: %{
      host: "syslog.example.com",
      port: 6514,
      tls: true,
      max_message_bytes: 1000
    }
  }

  test "every mapped backend has an exhaustive safe response policy" do
    mapped_types = Backend.adaptor_mapping() |> Map.keys() |> MapSet.new()

    assert mapped_types == BackendResponseConfig.safe_fields() |> Map.keys() |> MapSet.new()
    assert mapped_types == BackendResponseConfig.omitted_fields() |> Map.keys() |> MapSet.new()

    for {type, adaptor} <- Backend.adaptor_mapping() do
      config_fields = adaptor.cast_config(%{}).types |> Map.keys() |> MapSet.new()
      classified_fields = BackendResponseConfig.known_fields(type) |> MapSet.new()

      assert config_fields == classified_fields,
             "#{inspect(type)} response policy does not classify every adaptor config field"
    end
  end

  test "serializes every mapped backend type with exact safe fields only" do
    assert Map.keys(@configs) |> MapSet.new() ==
             Map.keys(@expected_response_configs) |> MapSet.new()

    Enum.each(@configs, fn {type, config} ->
      response_config = BackendResponseConfig.serialize(type, config)

      assert response_config == @expected_response_configs[type]
      refute Jason.encode!(response_config) =~ "synthetic-secret"
      refute Jason.encode!(response_config) =~ "synthetic-user"
      refute Map.has_key?(response_config, :headers)
      refute Map.has_key?(response_config, :metadata)
    end)
  end

  test "normalizes legacy string keys without atomizing unknown keys" do
    config = %{
      "url" => "https://example.com",
      "database" => "default",
      "port" => 8123,
      "username" => "synthetic-user",
      "password" => "synthetic-secret",
      "unknown_secret" => "synthetic-secret"
    }

    assert %{url: "https://example.com", database: "default", port: 8123} =
             BackendResponseConfig.serialize(:clickhouse, config)
  end

  test "omits unsafe urls and backend metadata" do
    for url <- [
          "https://user:synthetic-secret@example.com",
          "https://example.com:synthetic-secret",
          "https://exa mple.com",
          "https://%zz.example.com",
          "ftp://example.com",
          "https://example.com/path",
          "https://example.com?token=synthetic-secret",
          "https://example.com#synthetic-secret"
        ] do
      refute Map.has_key?(BackendResponseConfig.serialize(:webhook, %{url: url}), :url)
    end

    refute Map.has_key?(
             BackendResponseConfig.serialize(:clickhouse, %{
               read_only_urls: %{
                 "safe" => "https://read.example.com",
                 "unsafe" => "https://read.example.com/token"
               }
             }),
             :read_only_urls
           )

    response =
      Jason.encode!(%Backend{
        type: :webhook,
        config_encrypted: @configs.webhook,
        metadata: %{secret: "synthetic-secret"}
      })

    refute response =~ "synthetic-secret"
    refute response =~ "metadata"
  end

  test "fails closed for unknown type and malformed config" do
    assert BackendResponseConfig.serialize(:unknown, %{password: "synthetic-secret"}) == %{}
    assert BackendResponseConfig.serialize(:webhook, "synthetic-secret") == %{}
  end

  test "fails closed for missing callback, callback failures, and non-map callback results" do
    config = %{url: "https://example.com"}

    for adaptor <- [MissingRedactor, RaisingRedactor, ThrowingRedactor, NonMapRedactor] do
      assert BackendResponseConfig.serialize_with_adaptor(:webhook, adaptor, config) == %{}
    end
  end

  test "unloaded mapped adaptors are loaded before dispatch and preserve safe output" do
    adaptor = Backend.adaptor_mapping()[:datadog]

    try do
      :code.purge(adaptor)
      :code.delete(adaptor)
      assert :code.is_loaded(adaptor) == false

      assert %{region: "US1"} =
               unloaded_result = BackendResponseConfig.serialize(:datadog, @configs.datadog)

      assert {:file, _} = :code.is_loaded(adaptor)
      assert unloaded_result == BackendResponseConfig.serialize(:datadog, @configs.datadog)
    after
      Code.ensure_loaded(adaptor)
    end
  end
end
