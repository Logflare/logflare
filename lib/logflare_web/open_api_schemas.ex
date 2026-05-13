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
      source_mapping: %Schema{type: :object, nullable: true},
      sandboxable: %Schema{type: :boolean, nullable: true},
      cache_duration_seconds: %Schema{type: :integer},
      proactive_requerying_seconds: %Schema{type: :integer},
      max_limit: %Schema{type: :integer},
      enable_auth: %Schema{type: :boolean}
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
      default_ingest_backend_enabled?: %Schema{type: :boolean},
      inserted_at: %Schema{type: :string, format: :"date-time"},
      updated_at: %Schema{type: :string, format: :"date-time"}
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
      inserted_at: %Schema{type: :string, format: :"date-time"},
      updated_at: %Schema{type: :string, format: :"date-time"}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:name]
  end

  defmodule KeyValueApiSchema do
    @properties %{
      id: %Schema{type: :integer},
      key: %Schema{type: :string},
      value: %Schema{type: :object}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:key, :value]
  end

  defmodule BackendApiSchema do
    @properties %{
      name: %Schema{type: :string},
      id: %Schema{type: :integer},
      token: %Schema{type: :string},
      config: %Schema{type: :object},
      metadata: %Schema{type: :object},
      default_ingest?: %Schema{type: :boolean},
      inserted_at: %Schema{type: :string, format: :"date-time"},
      updated_at: %Schema{type: :string, format: :"date-time"}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:name]
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
      api_quota: %Schema{type: :integer},
      company: %Schema{type: :string, nullable: true},
      token: %Schema{type: :string}
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
