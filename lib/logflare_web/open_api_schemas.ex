defmodule LogflareWeb.OpenApiSchemas do
  alias OpenApiSpex.Schema

  defmodule EndpointQuery do
    @properties %{
      result: %Schema{type: :array, items: %Schema{type: :object}},
      error: %Schema{
        oneOf: [
          %Schema{type: :object},
          %Schema{type: :string}
        ]
      }
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule Event do
    @properties %{
      timestamp: %Schema{type: :integer},
      event_message: %Schema{type: :string}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule LogsCreated do
    @properties %{
      message: %Schema{type: :string, example: "Logged!"}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule QueryParseResult do
    @properties %{
      result: %Schema{type: :object},
      errors: %Schema{
        oneOf: [
          %Schema{type: :object},
          %Schema{type: :string}
        ]
      }
    }
    use LogflareWeb.OpenApi, properties: @properties, required: [:result]
  end

  defmodule QueryResult do
    @properties %{
      result: %Schema{type: :object},
      errors: %Schema{
        oneOf: [
          %Schema{type: :object},
          %Schema{type: :string}
        ]
      }
    }
    use LogflareWeb.OpenApi, properties: @properties, required: [:result]
  end

  defmodule EndpointApiSchema do
    @properties %{
      id: %Schema{type: :integer},
      description: %Schema{type: :string, nullable: true},
      token: %Schema{type: :string},
      name: %Schema{type: :string},
      query: %Schema{type: :string},
      language: %Schema{type: :string},
      source_mapping: %Schema{type: :object, nullable: true},
      sandboxable: %Schema{type: :boolean, nullable: true},
      cache_duration_seconds: %Schema{type: :integer},
      proactive_requerying_seconds: %Schema{type: :integer},
      max_limit: %Schema{type: :integer},
      enable_auth: %Schema{type: :boolean},
      redact_pii: %Schema{type: :boolean},
      enable_dynamic_reservation: %Schema{type: :boolean},
      labels: %Schema{type: :string, nullable: true},
      backend_id: %Schema{type: :integer, nullable: true}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: [:name, :query]
  end

  defmodule AccessToken do
    @properties %{
      id: %Schema{type: :integer},
      token: %Schema{type: :string},
      description: %Schema{type: :string},
      scopes: %Schema{type: :string},
      inserted_at: %Schema{type: :string, format: :"date-time"}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule Notification do
    @properties %{
      team_user_ids_for_email: %Schema{type: :array, items: %Schema{type: :string}},
      team_user_ids_for_sms: %Schema{type: :array, items: %Schema{type: :string}},
      team_user_ids_for_schema_updates: %Schema{type: :array, items: %Schema{type: :string}},
      other_email_notifications: %Schema{type: :string},
      user_email_notifications: %Schema{type: :boolean},
      user_text_notifications: %Schema{type: :boolean},
      user_schema_update_notifications: %Schema{type: :boolean}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule Source do
    @properties %{
      name: %Schema{type: :string},
      description: %Schema{type: :string, nullable: true},
      service_name: %Schema{type: :string, nullable: true},
      token: %Schema{type: :string},
      id: %Schema{type: :integer},
      favorite: %Schema{type: :boolean},
      webhook_notification_url: %Schema{type: :string},
      api_quota: %Schema{type: :integer},
      slack_hook_url: %Schema{type: :string},
      bigquery_table_ttl: %Schema{type: :integer},
      public_token: %Schema{type: :string},
      bq_table_id: %Schema{type: :string},
      has_rejected_events: %Schema{type: :boolean},
      metrics: %Schema{type: :object},
      notifications: %Schema{type: :object, items: Notification},
      custom_event_message_keys: %Schema{type: :string},
      backends: %Schema{type: :array, items: LogflareWeb.OpenApiSchemas.BackendApiSchema},
      retention_days: %Schema{type: :integer, nullable: true},
      transform_copy_fields: %Schema{type: :string, nullable: true},
      transform_key_values: %Schema{type: :string, nullable: true},
      transform_drop_fields: %Schema{type: :string, nullable: true},
      bigquery_clustering_fields: %Schema{type: :string, nullable: true},
      default_ingest_backend_enabled?: %Schema{type: :boolean},
      notifications_every: %Schema{type: :integer},
      lock_schema: %Schema{type: :boolean},
      validate_schema: %Schema{type: :boolean},
      drop_lql_string: %Schema{type: :string, nullable: true},
      default_search_lql: %Schema{type: :string, nullable: true},
      suggested_keys: %Schema{type: :string, nullable: true},
      disable_tailing: %Schema{type: :boolean, nullable: true},
      bq_storage_write_api: %Schema{type: :boolean, nullable: true},
      labels: %Schema{type: :string, nullable: true}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:name]
  end

  defmodule SourceSchema do
    @properties %{}

    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule RuleApiSchema do
    @properties %{
      id: %Schema{type: :integer},
      token: %Schema{type: :string},
      lql_string: %Schema{type: :string},
      backend_id: %Schema{type: :integer},
      source_id: %Schema{type: :integer},
      sink: %Schema{type: :string, nullable: true}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:lql_string]
  end

  defmodule KeyValueApiSchema do
    @properties %{
      id: %Schema{type: :integer},
      key: %Schema{type: :string},
      value: %Schema{type: :object}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:key, :value]
  end

  defmodule WebhookConfigSchema do
    @properties %{
      url: %Schema{type: :string},
      headers: %Schema{type: :object},
      http: %Schema{type: :string, nullable: true},
      gzip: %Schema{type: :boolean}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:url]
  end

  defmodule DatadogConfigSchema do
    @properties %{
      api_key: %Schema{type: :string},
      region: %Schema{type: :string}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:api_key, :region]
  end

  defmodule SentryConfigSchema do
    @properties %{
      dsn: %Schema{type: :string}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:dsn]
  end

  defmodule PostgresConfigSchema do
    @properties %{
      url: %Schema{type: :string, nullable: true},
      username: %Schema{type: :string, nullable: true},
      password: %Schema{type: :string, nullable: true},
      hostname: %Schema{type: :string, nullable: true},
      database: %Schema{type: :string, nullable: true},
      schema: %Schema{type: :string, nullable: true},
      port: %Schema{type: :integer, nullable: true},
      pool_size: %Schema{type: :integer, nullable: true}
    }

    # Either url, or hostname plus the other connection fields, must be
    # provided -- Postgres.Adaptor.validate_config/1 enforces this at the
    # changeset level; it isn't expressible as a plain `required` list.
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule BigQueryConfigSchema do
    @properties %{
      project_id: %Schema{type: :string, nullable: true},
      dataset_id: %Schema{type: :string, nullable: true}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule LokiConfigSchema do
    @properties %{
      url: %Schema{type: :string},
      headers: %Schema{type: :object},
      username: %Schema{type: :string, nullable: true},
      password: %Schema{type: :string, nullable: true}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:url]
  end

  defmodule ClickhouseConfigSchema do
    @properties %{
      url: %Schema{type: :string},
      database: %Schema{type: :string},
      port: %Schema{type: :integer},
      username: %Schema{type: :string, nullable: true},
      password: %Schema{type: :string, nullable: true},
      pool_size: %Schema{type: :integer, nullable: true},
      read_only_url: %Schema{type: :string, nullable: true},
      insert_protocol: %Schema{type: :string, nullable: true},
      native_port: %Schema{type: :integer, nullable: true},
      native_pool_size: %Schema{type: :integer, nullable: true}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:url, :database, :port]
  end

  defmodule IncidentioConfigSchema do
    @properties %{
      api_token: %Schema{type: :string},
      alert_source_config_id: %Schema{type: :string},
      metadata: %Schema{type: :object}
    }

    use LogflareWeb.OpenApi,
      properties: @properties,
      required: [:api_token, :alert_source_config_id]
  end

  defmodule S3ConfigSchema do
    @properties %{
      endpoint: %Schema{type: :string, nullable: true},
      s3_bucket: %Schema{type: :string},
      storage_region: %Schema{type: :string},
      access_key_id: %Schema{type: :string},
      secret_access_key: %Schema{type: :string},
      batch_timeout: %Schema{type: :integer, nullable: true}
    }

    use LogflareWeb.OpenApi,
      properties: @properties,
      required: [:s3_bucket, :storage_region, :access_key_id, :secret_access_key]
  end

  defmodule AxiomConfigSchema do
    @properties %{
      domain: %Schema{type: :string},
      api_token: %Schema{type: :string},
      dataset_name: %Schema{type: :string}
    }

    use LogflareWeb.OpenApi,
      properties: @properties,
      required: [:domain, :api_token, :dataset_name]
  end

  defmodule OtlpConfigSchema do
    @properties %{
      endpoint: %Schema{type: :string},
      protocol: %Schema{type: :string, nullable: true},
      gzip: %Schema{type: :boolean},
      headers: %Schema{type: :object}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:endpoint]
  end

  defmodule Last9ConfigSchema do
    @properties %{
      region: %Schema{type: :string},
      username: %Schema{type: :string},
      password: %Schema{type: :string}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:region, :username, :password]
  end

  defmodule SyslogConfigSchema do
    @properties %{
      host: %Schema{type: :string},
      port: %Schema{type: :integer},
      tls: %Schema{type: :boolean},
      cipher_key: %Schema{type: :string, nullable: true},
      ca_cert: %Schema{type: :string, nullable: true},
      client_cert: %Schema{type: :string, nullable: true},
      client_key: %Schema{type: :string, nullable: true},
      structured_data: %Schema{type: :string, nullable: true},
      max_message_bytes: %Schema{type: :integer, nullable: true}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:host, :port]
  end

  defmodule BackendApiSchema do
    @properties %{
      name: %Schema{type: :string},
      id: %Schema{type: :integer},
      token: %Schema{type: :string},
      type: %Schema{type: :string},
      description: %Schema{type: :string, nullable: true},
      config: %Schema{
        anyOf: [
          LogflareWeb.OpenApiSchemas.WebhookConfigSchema,
          LogflareWeb.OpenApiSchemas.DatadogConfigSchema,
          LogflareWeb.OpenApiSchemas.SentryConfigSchema,
          LogflareWeb.OpenApiSchemas.PostgresConfigSchema,
          LogflareWeb.OpenApiSchemas.BigQueryConfigSchema,
          LogflareWeb.OpenApiSchemas.LokiConfigSchema,
          LogflareWeb.OpenApiSchemas.ClickhouseConfigSchema,
          LogflareWeb.OpenApiSchemas.IncidentioConfigSchema,
          LogflareWeb.OpenApiSchemas.S3ConfigSchema,
          LogflareWeb.OpenApiSchemas.AxiomConfigSchema,
          LogflareWeb.OpenApiSchemas.OtlpConfigSchema,
          LogflareWeb.OpenApiSchemas.Last9ConfigSchema,
          LogflareWeb.OpenApiSchemas.SyslogConfigSchema
        ]
      },
      metadata: %Schema{type: :object},
      default_ingest?: %Schema{type: :boolean},
      inserted_at: %Schema{type: :string, format: :"date-time"},
      updated_at: %Schema{type: :string, format: :"date-time"}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:name, :type, :config]
  end

  defmodule BackendConnectionTest do
    @properties %{
      connected?: %Schema{type: :boolean},
      reason: %Schema{type: :string}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:connected?]
  end

  defmodule User do
    @properties %{
      email: %Schema{type: :string},
      provider: %Schema{type: :string},
      api_key: %Schema{type: :string},
      email_preferred: %Schema{type: :string},
      name: %Schema{type: :string},
      image: %Schema{type: :string, nullable: true},
      email_me_product: %Schema{type: :boolean},
      phone: %Schema{type: :string, nullable: true},
      bigquery_project_id: %Schema{type: :string, nullable: true},
      bigquery_dataset_location: %Schema{type: :string, nullable: true},
      bigquery_dataset_id: %Schema{type: :string, nullable: true},
      bigquery_reservation_search: %Schema{type: :string, nullable: true},
      bigquery_reservation_alerts: %Schema{type: :string, nullable: true},
      bigquery_additional_projects: %Schema{type: :string, nullable: true},
      api_quota: %Schema{type: :integer},
      company: %Schema{type: :string, nullable: true},
      token: %Schema{type: :string},
      metadata: %Schema{type: :object, nullable: true},
      partner_upgraded: %Schema{type: :boolean, nullable: true},
      system_monitoring: %Schema{type: :boolean}
    }
    use LogflareWeb.OpenApi,
      properties: @properties,
      required: [:email, :provider, :token, :api_key]
  end

  defmodule TeamUser do
    @properties %{
      email: %Schema{type: :string},
      name: %Schema{type: :string}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:email, :name]
  end

  defmodule Team do
    @properties %{
      name: %Schema{type: :string},
      token: %Schema{type: :string},
      user: User,
      team_users: %Schema{type: :array, items: TeamUser}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: [:name]
  end
end
