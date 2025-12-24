defmodule LogflareWeb.SearchLive.LogEventsComponentsTest do
  use LogflareWeb.ConnCase, async: true

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

  @default_attrs %{
    search_op_log_events: nil,
    last_query_completed_at: nil,
    loading: false,
    search_timezone: "Etc/UTC",
    tailing?: false,
    querystring: "",
    lql_rules: [],
    source: nil
  }

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
          build(:log_event, message: "Log message 1", metadata: %{user_id: 123}, source: source)
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
          @default_attrs
          | lql_rules: lql_rules,
            source: source,
            search_op_log_events: search_op_log_events
        })

      assert html =~ "Log message 1"
    end

    test "renders loading state", %{source: source, lql_rules: lql_rules} do
      html =
        render_component(&LogEventComponents.logs_list/1, %{
          @default_attrs
          | loading: true,
            source: source,
            lql_rules: lql_rules
        })

      # Assert loading state is rendered
      assert html =~ ~s|id="logs-list-loading"|

      # Assert logs list is NOT rendered
      refute html =~ ~s|id="logs-list"|
    end

    test "renders empty state when no log events", %{source: source, lql_rules: lql_rules} do
      html =
        render_component(&LogEventComponents.logs_list/1, %{
          @default_attrs
          | lql_rules: lql_rules,
            source: source,
            loading: false,
            search_op_log_events: nil
        })

      # Assert logs list ul is NOT rendered when search_op_log_events is nil
      refute html =~ ~s|id="logs-list"|
    end

    test "renders placeholder for log events with nil event_message", %{
      source: source,
      lql_rules: lql_rules
    } do
      log_event_without_message = %Logflare.LogEvent{
        id: Ecto.UUID.generate(),
        body: %{
          "timestamp" => System.system_time(:microsecond),
          "id" => Ecto.UUID.generate(),
          "metadata" => %{"user_id" => 456}
        },
        source_id: source.id,
        valid: true
      }

      search_op_log_events = %{rows: [log_event_without_message]}

      html =
        render_component(&LogEventComponents.logs_list/1, %{
          @default_attrs
          | lql_rules: lql_rules,
            source: source,
            search_op_log_events: search_op_log_events
        })

      assert html =~ "(empty event message)"
      assert html =~ "tw-italic"
      assert html =~ "tw-text-gray-500"
    end

    test "renders both normal and nil event_message log events", %{
      source: source,
      lql_rules: lql_rules
    } do
      normal_log_event =
        build(:log_event,
          message: "Normal log message",
          metadata: %{user_id: 123},
          source: source
        )

      log_event_without_message = %Logflare.LogEvent{
        id: Ecto.UUID.generate(),
        body: %{
          "timestamp" => System.system_time(:microsecond),
          "id" => Ecto.UUID.generate(),
          "metadata" => %{"user_id" => 789}
        },
        source_id: source.id,
        valid: true
      }

      search_op_log_events = %{rows: [normal_log_event, log_event_without_message]}

      html =
        render_component(&LogEventComponents.logs_list/1, %{
          @default_attrs
          | lql_rules: lql_rules,
            source: source,
            search_op_log_events: search_op_log_events
        })

      assert html =~ "Normal log message"
      assert html =~ "(empty event message)"
    end
  end
end
