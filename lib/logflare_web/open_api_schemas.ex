defmodule LogflareWeb.OpenApiSchemas do
  alias OpenApiSpex.Schema

  defmodule EndpointQuery do
    @properties %{
      result: %Schema{type: :string, example: "Logged!"},
      errors: %Schema{
        required: false,
        oneOf: [
          %Schema{type: :object},
          %Schema{type: :string}
        ]
      }
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule LogsCreated do
    @properties %{
      message: %Schema{type: :string, example: "Logged!"}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: []
  end

  defmodule Endpoint do
    @properties %{
      token: %Schema{type: :string},
      name: %Schema{type: :string},
      query: %Schema{type: :string},
      source_mapping: %Schema{type: :object},
      sandboxable: %Schema{type: :boolean},
      cache_duration_seconds: %Schema{type: :integer},
      proactive_requerying_seconds: %Schema{type: :integer},
      max_limit: %Schema{type: :integer},
      enable_auth: %Schema{type: :boolean},
      inserted_at: %Schema{type: :string, format: :"date-time"},
      updated_at: %Schema{type: :string, format: :"date-time"}
    }
    use LogflareWeb.OpenApi, properties: @properties, required: [:name, :query]
  end

  defmodule Notification do
    @properties %{
      team_user_ids_for_email: %Schema{type: :array, allOf: %Schema{type: :string}},
      team_user_ids_for_sms: %Schema{type: :array, allOf: %Schema{type: :string}},
      team_user_ids_for_schema_updates: %Schema{type: :array, allOf: %Schema{type: :string}},
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
      token: %Schema{type: :string},
      id: %Schema{},
      favorite: %Schema{type: :boolean},
      webhook_notification_url: %Schema{type: :string},
      api_quota: %Schema{type: :integer},
      slack_hook_url: %Schema{type: :string},
      bigquery_table_ttl: %Schema{type: :integer},
      public_token: %Schema{type: :string},
      bq_table_id: %Schema{type: :string},
      bq_table_schema: %Schema{type: :object},
      has_rejected_events: %Schema{type: :boolean},
      metrics: %Schema{type: :object},
      notifications: %Schema{type: :array, items: Notification},
      custom_event_message_keys: %Schema{type: :string},
      inserted_at: %Schema{type: :string, format: :"date-time"},
      updated_at: %Schema{type: :string, format: :"date-time"}
    }

    use LogflareWeb.OpenApi, properties: @properties, required: [:name]
  end

  defmodule User do
    @properties %{
      email: %Schema{type: :string},
      provider: %Schema{type: :string},
      api_key: %Schema{type: :string},
      email_preferred: %Schema{type: :string},
      name: %Schema{type: :string},
      image: %Schema{type: :string},
      email_me_product: %Schema{type: :boolean},
      phone: %Schema{type: :string},
      bigquery_project_id: %Schema{type: :string},
      bigquery_dataset_location: %Schema{type: :string},
      bigquery_dataset_id: %Schema{type: :string},
      api_quota: %Schema{type: :integer},
      company: %Schema{type: :string},
      token: %Schema{type: :string}
    }
    use LogflareWeb.OpenApi,
      properties: @properties,
      required: [:email, :provider, :token, :provider_uid, :api_key]
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
