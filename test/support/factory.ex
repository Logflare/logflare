defmodule Logflare.Factory do
  @moduledoc """
  Generates fixtures for schemas
  """
  use ExMachina.Ecto, repo: Logflare.Repo

  alias Logflare.Backends.Backend
  alias Logflare.Billing.BillingAccount
  alias Logflare.Billing.BillingCount
  alias Logflare.Billing.PaymentMethod
  alias Logflare.Billing.Plan
  alias Logflare.Endpoints.Query
  alias Logflare.LogEvent
  alias Logflare.Lql
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.Partners.Partner
  alias Logflare.Rules.Rule
  alias Logflare.Source
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Teams.Team
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.TestUtils
  alias Logflare.User
  alias Logflare.Users.UserPreferences
  alias Logflare.Alerting.AlertQuery

  def user_factory do
    email = "#{TestUtils.random_string(8)}@#{TestUtils.random_string()}.com"

    %User{
      name: "JaneJohn Jones #{TestUtils.random_string()}",
      email: email,
      email_preferred: email,
      provider: "google",
      bigquery_processed_bytes_limit: 10_000_000_000,
      token: TestUtils.gen_uuid(),
      api_key: TestUtils.random_string(10),
      provider_uid: "provider_uid_#{TestUtils.random_string()}"
    }
  end

  def team_factory do
    %Team{
      name: "my team #{TestUtils.random_string()}",
      user: build(:user)
    }
  end

  def team_user_factory do
    email = "#{TestUtils.random_string(8)}@#{TestUtils.random_string()}.com"

    %TeamUser{
      name: "some name #{TestUtils.random_string()}",
      team: build(:team),
      provider: "google",
      email: email,
      provider_uid: "provider_uid_#{TestUtils.random_string()}"
    }
  end

  def source_factory do
    %Source{
      name: TestUtils.random_string(10),
      token: TestUtils.gen_uuid_atom(),
      rules: [],
      favorite: false,
      metrics: %{
        avg: 0
      },
      notifications: %{
        user_schema_update_notifications: true
      }
    }
  end

  def source_schema_factory(attrs) do
    %SourceSchema{
      bigquery_schema: attrs[:bigquery_schema] || TestUtils.default_bq_schema(),
      schema_flat_map:
        Logflare.Google.BigQuery.SchemaUtils.bq_schema_to_flat_typemap(
          attrs[:bigquery_schema] || TestUtils.default_bq_schema()
        )
    }
    |> merge_attributes(attrs)
  end

  def backend_factory(attrs) do
    config =
      attrs[:config] || attrs[:config_encrypted] ||
        %{
          project_id: TestUtils.random_string(),
          dataset_id: TestUtils.random_string()
        }

    %Backend{
      name: TestUtils.random_string(),
      description: attrs[:description],
      type: attrs[:type] || :bigquery,
      config_encrypted: config,
      config: config,
      sources: attrs[:sources] || [],
      rules: attrs[:rules] || [],
      user_id: attrs[:user_id],
      user: attrs[:user],
      metadata: attrs[:metadata] || nil,
      default_ingest?: attrs[:default_ingest?] || false,
      updated_at: attrs[:updated_at],
      inserted_at: attrs[:inserted_at],
      alert_queries: attrs[:alert_queries] || []
    }
  end

  def postgres_backend_factory do
    %Backend{
      name: TestUtils.random_string(),
      type: :postgres,
      config: %{
        url: "postgresql://#{TestUtils.random_string()}"
      }
    }
  end

  def rule_factory(attrs) do
    lql = Map.get(attrs, :lql_string) || Map.get(attrs, "lql_string", "testing")
    {:ok, lql_filters} = Lql.Parser.parse(lql, TestUtils.default_bq_schema())

    %Rule{
      lql_string: lql,
      lql_filters: lql_filters,
      sink: attrs[:sink],
      source_id: attrs[:source_id],
      source: attrs[:source],
      backend: attrs[:backend],
      backend_id: attrs[:backend_id]
    }
  end

  def log_event_factory(attrs) do
    {source, attrs} = Map.pop(attrs, :source, build(:source))
    {ingested_at, params} = Map.pop(attrs, :ingested_at)

    params =
      for {k, v} <- params, into: %{} do
        case k do
          k when is_atom(k) -> {Atom.to_string(k), v}
          _ -> {k, v}
        end
      end

    params =
      Map.merge(
        params,
        %{
          "message" =>
            params["message"] || params["event_message"] ||
              Map.get(params, "event_message", "test-msg"),
          "timestamp" =>
            params["timestamp"] || params[:timestamp] || DateTime.utc_now() |> to_string,
          "metadata" => params["metadata"] || params[:metadata] || %{}
        }
      )
      |> Map.drop([:metadata, :event_message, :message, :timestamp])

    LogEvent.make(params, %{source: source})
    |> Map.update!(:ingested_at, fn v ->
      if ingested_at, do: ingested_at, else: v
    end)
  end

  def plan_factory() do
    %Plan{
      stripe_id: "31415",
      price: 0,
      period: "month",
      name: "Free"
    }
  end

  def billing_account_factory(attrs) do
    stripe_plan_id = Map.get(attrs, :stripe_plan_id, "some plan id #{TestUtils.random_string()}")

    stripe_sub_item_id =
      Map.get(attrs, :stripe_subscription_item_id, "some sub id #{TestUtils.random_string()}")

    attrs =
      attrs
      |> Map.delete(:stripe_plan_id)
      |> Map.delete(:stripe_subscription_item_id)

    %BillingAccount{
      user: build(:user),
      stripe_customer: TestUtils.random_string(10),
      stripe_subscriptions: %{
        "data" => [
          %{
            "plan" => %{"id" => stripe_plan_id},
            "items" => %{
              "data" => [%{"id" => stripe_sub_item_id}]
            }
          }
        ]
      }
    }
    |> merge_attributes(attrs)
  end

  def payment_method_factory(attrs) do
    customer_id = Map.get(attrs, :customer_id)

    customer_id =
      if customer_id == nil do
        ba = build(:billing_account)
        Map.get(ba, :stripe_customer)
      else
        customer_id
      end

    %PaymentMethod{
      stripe_id: "stripe_#{TestUtils.random_string()}",
      customer_id: customer_id,
      price_id: "price_#{TestUtils.random_string()}"
    }
    |> merge_attributes(attrs)
  end

  @spec user_preferences_factory :: Logflare.Users.UserPreferences.t()
  def user_preferences_factory() do
    %UserPreferences{}
  end

  def endpoint_factory(attrs \\ %{}) do
    user = Map.get(attrs, :user, build(:user))
    backend = Map.get(attrs, :backend)
    language = Map.get(attrs, :language, :bq_sql)

    %Query{
      user: user,
      description: "some desc #{TestUtils.random_string()}",
      token: Ecto.UUID.generate(),
      query: "select current_date() as date",
      language: language,
      backend: backend,
      name: TestUtils.random_string()
    }
    |> merge_attributes(Map.drop(attrs, [:backend, :language, :user]))
  end

  def child_endpoint_factory do
    %Query{
      user: build(:user),
      token: Ecto.UUID.generate(),
      query: "select current_date() as date",
      name: TestUtils.random_string()
    }
  end

  def access_token_factory do
    %OauthAccessToken{
      token: TestUtils.random_string(20),
      resource_owner: build(:user),
      scopes: "ingest"
    }
  end

  def public_access_token_factory do
    %OauthAccessToken{
      token: TestUtils.random_string(20),
      resource_owner: build(:user),
      scopes: ~w(ingest)
    }
  end

  def private_access_token_factory do
    %OauthAccessToken{
      token: TestUtils.random_string(20),
      resource_owner: build(:user),
      scopes: ~w(private)
    }
  end

  def partner_access_token_factory do
    %OauthAccessToken{
      token: TestUtils.random_string(20),
      resource_owner: build(:partner),
      scopes: ~w(partner)
    }
  end

  def user_without_billing_account_factory() do
    build(:user,
      valid_google_account: true,
      billing_account: nil,
      provider: "google",
      team: insert(:team)
    )
  end

  def user_without_stripe_subscription_factory() do
    build(:user,
      provider: "google",
      valid_google_account: true,
      billing_account: insert(:billing_account, stripe_subscriptions: nil),
      team: insert(:team)
    )
  end

  def user_with_wrong_stripe_sub_content_factory() do
    build(:user,
      provider: "google",
      valid_google_account: true,
      billing_account: insert(:billing_account, stripe_subscriptions: %{"invalid" => ""}),
      team: insert(:team)
    )
  end

  def user_with_lifetime_account_non_google_factory() do
    build(:user,
      provider: "potato",
      billing_account: insert(:billing_account, lifetime_plan: true),
      team: insert(:team)
    )
  end

  def user_with_lifetime_account_factory() do
    build(:user,
      provider: "google",
      valid_google_account: true,
      billing_account: insert(:billing_account, lifetime_plan: true),
      team: insert(:team)
    )
  end

  def user_with_legacy_account_factory() do
    build(:user,
      provider: "google",
      valid_google_account: true,
      billing_enabled: false,
      team: insert(:team)
    )
  end

  def user_with_stripe_subscription_factory(
        %{stripe_id: stripe_id} \\ %{stripe_id: TestUtils.random_string()}
      ) do
    build(:user,
      provider: "google",
      valid_google_account: true,
      billing_account:
        insert(:billing_account,
          stripe_subscriptions: %{"data" => [%{"plan" => %{"id" => stripe_id}}]}
        ),
      team: insert(:team)
    )
  end

  def billing_counts_factory() do
    user = insert(:user)
    source = build(:source, user: user)

    %BillingCount{
      count: TestUtils.random_pos_integer(),
      node: TestUtils.random_string(8),
      user: user,
      source: source
    }
  end

  def partner_factory() do
    %Partner{
      name: TestUtils.random_string()
    }
  end

  def alert_factory() do
    %AlertQuery{
      name: "some name",
      cron: "0 0 1 * *",
      query: "select current_date() as date",
      slack_hook_url: "some slack_hook_url",
      source_mapping: %{},
      webhook_notification_url: "some webhook_notification_url",
      language: :bq_sql
    }
  end
end
