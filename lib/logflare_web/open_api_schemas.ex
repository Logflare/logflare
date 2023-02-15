defmodule LogflareWeb.OpenApiSchemas do
  alias OpenApiSpex.Schema

  defmodule Endpoint do
    require OpenApiSpex

    OpenApiSpex.schema(%{
      type: :object,
      properties: %{
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
      },
      required: [:name, :query]
    })

    def response(), do: {"Endpoint Response", "application/json", __MODULE__}
    def params(), do: {"Endpoint Params", "application/json", __MODULE__}
  end

  defmodule Notification do
    require OpenApiSpex

    OpenApiSpex.schema(%{
      type: :object,
      properties: %{
        team_user_ids_for_email: %Schema{type: :array, allOf: %Schema{type: :string}},
        team_user_ids_for_sms: %Schema{type: :array, allOf: %Schema{type: :string}},
        team_user_ids_for_schema_updates: %Schema{type: :array, allOf: %Schema{type: :string}},
        other_email_notifications: %Schema{type: :string},
        user_email_notifications: %Schema{type: :boolean},
        user_text_notifications: %Schema{type: :boolean},
        user_schema_update_notifications: %Schema{type: :boolean}
      }
    })

    def response(), do: {"Notification Response", "application/json", __MODULE__}
  end

  defmodule Source do
    require OpenApiSpex

    OpenApiSpex.schema(%{
      type: :object,
      properties: %{
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
      },
      required: [:name]
    })

    def response(), do: {"Source Response", "application/json", __MODULE__}
    def params(), do: {"Source Params", "application/json", __MODULE__}
  end

  defmodule SourceList do
    require OpenApiSpex

    OpenApiSpex.schema(%{type: :array, items: Source})
    def response(), do: {"Source List Response", "application/json", __MODULE__}
  end

  defmodule EndpointList do
    require OpenApiSpex

    OpenApiSpex.schema(%{type: :array, items: Endpoint})
    def response(), do: {"Endpoint List Response", "application/json", __MODULE__}
  end

  defmodule Created do
    def response(schema), do: {"Created Response", "application/json", schema}
  end

  defmodule Accepted do
    require OpenApiSpex
    OpenApiSpex.schema(%{})

    def response(), do: {"Accepted Response", "text/plain", __MODULE__}
  end

  defmodule NotFound do
    require OpenApiSpex
    OpenApiSpex.schema(%{})

    def response(), do: {"Not found", "text/plain", __MODULE__}
  end
end
