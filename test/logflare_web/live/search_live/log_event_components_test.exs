defmodule LogflareWeb.SearchLive.LogEventComponentsTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.LiveViewTest
  import Phoenix.Component

  alias Logflare.Lql
  alias Logflare.Sources.Source
  alias LogflareWeb.SearchLive.LogEventComponents

  @default_attrs %{
    search_op_log_events: nil,
    last_query_completed_at: nil,
    loading: false,
    search_timezone: "Etc/UTC",
    tailing?: false,
    querystring: "",
    lql_rules: [],
    source: nil,
    search_op: nil
  }

  defmodule TestLive do
    use LogflareWeb, :live_view

    def render(assigns) do
      ~H"""
      <div>
        <LogEventComponents.results_list
          search_op_log_events={@search_op_log_events}
          search_op={@search_op}
          last_query_completed_at={@last_query_completed_at}
          loading={@loading}
          search_timezone={@search_timezone}
          tailing?={@tailing?}
          querystring={@querystring}
        />
      </div>
      """
    end

    def mount(_params, session, socket) do
      {:ok,
       assign(socket,
         search_op_log_events: session["search_op_log_events"],
         search_op: session["search_op"],
         last_query_completed_at: session["last_query_completed_at"],
         loading: session["loading"],
         search_timezone: session["search_timezone"],
         tailing?: session["tailing?"],
         querystring: session["querystring"]
       )}
    end
  end

  describe "results_list/1" do
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
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op: %{source: source, lql_rules: lql_rules, search_timezone: "Etc/UTC"},
            search_op_log_events: search_op_log_events
        })

      assert html =~ "Log message 1"
    end

    test "renders loading state", %{source: source, lql_rules: lql_rules} do
      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | loading: true,
            search_op: %{source: source, lql_rules: lql_rules, search_timezone: "Etc/UTC"},
            search_op_log_events: %{rows: []}
        })

      assert html =~ ~r|id="logs-list" class="(.*)blurred"|
    end

    test "renders empty state when no log events", %{source: source, lql_rules: lql_rules} do
      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op: %{source: source, lql_rules: lql_rules, search_timezone: "Etc/UTC"},
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
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op: %{source: source, lql_rules: lql_rules, search_timezone: "Etc/UTC"},
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
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op: %{source: source, lql_rules: lql_rules, search_timezone: "Etc/UTC"},
            search_op_log_events: search_op_log_events
        })

      assert html =~ "Normal log message"
      assert html =~ "(empty event message)"
    end
  end

  describe "selected_fields/1" do
    test "renders selected fields with display names and values" do
      log_event =
        build(:log_event,
          metadata_user_id: "user_123",
          metadata_store_city: "San Francisco",
          food: "Pizza"
        )

      select_fields = [
        %{display: "metadata.user_id", key: "metadata_user_id"},
        %{display: "metadata.store.city", key: "metadata_store_city"},
        %{display: "food", key: "food"}
      ]

      assigns = %{
        log_event: log_event,
        select_fields: select_fields
      }

      html =
        rendered_to_string(~H"""
        <LogEventComponents.selected_fields log_event={@log_event} select_fields={@select_fields} />
        """)

      assert html =~ "user_id"
      assert html =~ "user_123"
      assert html =~ "city"
      assert html =~ "San Francisco"
      assert html =~ "food"
      assert html =~ "Pizza"
    end

    test "handles null values" do
      log_event = build(:log_event, metadata_user_id: nil)

      select_fields = [
        %{display: "metadata.user_id", key: "metadata_user_id"}
      ]

      assigns = %{
        log_event: log_event,
        select_fields: select_fields
      }

      html =
        rendered_to_string(~H"""
        <LogEventComponents.selected_fields log_event={@log_event} select_fields={@select_fields} />
        """)

      assert html =~ "metadata.user_id"
      assert html =~ "null"
    end
  end

  describe "lql_with_recommended_fields/3" do
    test "normalizes required marker from suggested keys" do
      user = insert(:user)
      source = insert(:source, user: user, suggested_keys: "project!")
      event = build(:log_event, source: source, project: "my-project")

      lql =
        LogEventComponents.lql_with_recommended_fields(
          [],
          event,
          source
        )

      schema =
        %{"project" => "my-project"}
        |> Source.BigQuery.SchemaBuilder.build_table_schema(
          Source.BigQuery.SchemaBuilder.initial_table_schema()
        )

      {:ok, rules} = Lql.decode(lql, schema)
      filter_paths = rules |> Lql.Rules.get_filter_rules() |> Enum.map(& &1.path)

      assert "project" in filter_paths
      refute "project!" in filter_paths
    end
  end

  describe "formatted_for_clipboard/2" do
    test "formats log event with select fields for clipboard" do
      log_event =
        build(:log_event,
          event_message: "User login successful",
          metadata_user_id: "user_123",
          city: "San Francisco",
          timestamp: 1_234_567_890_000,
          long_field: String.duplicate("a", 80)
        )

      lql_rules = [
        %Logflare.Lql.Rules.SelectRule{path: "metadata.user_id", alias: nil},
        %Logflare.Lql.Rules.SelectRule{path: "metadata.store.city", alias: "city"},
        %Logflare.Lql.Rules.SelectRule{path: "long_field", alias: nil}
      ]

      search_op = %{
        lql_rules: lql_rules,
        search_timezone: "America/Los_Angeles"
      }

      assert LogEventComponents.formatted_for_clipboard(log_event, search_op) =~ """
             Fri Feb 13 2009 15:31:30-08:00    User login successful

             user_id: user_123

             city: San Francisco

             long_field:
             aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

             """
    end
  end
end
