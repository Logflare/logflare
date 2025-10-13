defmodule LogflareWeb.SearchLive.LogEventsComponentsTest do
  use LogflareWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Logflare.Lql
  alias Logflare.Sources.Source
  alias LogflareWeb.SearchLive.LogEventComponents

  defmodule TestLive do
    use LogflareWeb, :live_view

    def render(assigns) do
      ~H"""
      <div>
        <LogEventComponents.logs_list
          search_op_log_events={@search_op_log_events}
          last_query_completed_at={@last_query_completed_at}
          loading={@loading}
          search_timezone={@search_timezone}
          source={@source}
          tailing?={@tailing?}
          querystring={@querystring}
          lql_rules={@lql_rules}
        />
      </div>
      """
    end

    def mount(_params, session, socket) do
      {:ok,
       assign(socket,
         search_op_log_events: session["search_op_log_events"],
         last_query_completed_at: session["last_query_completed_at"],
         loading: session["loading"],
         search_timezone: session["search_timezone"],
         source: session["source"],
         tailing?: session["tailing?"],
         querystring: session["querystring"],
         lql_rules: session["lql_rules"]
       )}
    end
  end

  describe "logs_list/1" do
    setup do
      user = insert(:user)

      source =
        insert(:source,
          user: user,
          suggested_keys: "m.user_id",
          bigquery_clustering_fields: "session_id"
        )

      search_op_log_events = %{
        rows: [
          build(:log_event, message: "Log message 1", metadata: %{user_id: 123}, source: source),
          build(:log_event,
            message: Jason.encode!(%{session_id: "abc123"}),
            metadata: %{user_id: 123},
            source: source
          )
        ]
      }

      {:ok, lql_rules} =
        Lql.decode(
          "c:count(*) c:group_by(t::minute)",
          Source.BigQuery.SchemaBuilder.initial_table_schema()
        )

      [
        source: source,
        search_op_log_events: search_op_log_events,
        lql_rules: lql_rules
      ]
    end

    test "renders log events list", %{
      source: source,
      search_op_log_events: search_op_log_events,
      lql_rules: lql_rules
    } do
      html =
        render_component(&LogEventComponents.logs_list/1, %{
          search_op_log_events: search_op_log_events,
          last_query_completed_at: DateTime.utc_now() |> DateTime.to_unix(),
          loading: false,
          search_timezone: "America/New_York",
          source: source,
          tailing?: false,
          querystring: "",
          lql_rules: lql_rules
        })

      assert html =~ "Log message 1"
      assert html =~ Plug.HTML.html_escape(~s|{"session_id":"abc123"}|)
    end

    test "renders loading state", %{source: source, lql_rules: lql_rules} do
      html =
        render_component(&LogEventComponents.logs_list/1, %{
          search_op_log_events: nil,
          last_query_completed_at: nil,
          loading: true,
          search_timezone: "America/New_York",
          source: source,
          tailing?: false,
          querystring: "",
          lql_rules: lql_rules
        })

      # Assert loading state is rendered
      assert html =~ ~s|class="blurred list-unstyled console-text-list"|

      # Assert logs list is NOT rendered
      refute html =~ ~s|<ul|
    end

    test "renders empty state when no log events", %{source: source, lql_rules: lql_rules} do
      html =
        render_component(&LogEventComponents.logs_list/1, %{
          search_op_log_events: nil,
          last_query_completed_at: nil,
          loading: false,
          search_timezone: "America/New_York",
          source: source,
          tailing?: false,
          querystring: "",
          lql_rules: lql_rules
        })

      # Assert logs list ul is NOT rendered when search_op_log_events is nil
      refute html =~ ~s|id="logs-list"|
    end
  end
end
