defmodule LogflareWeb.SearchLive.LogEventComponentsTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.LiveViewTest
  import Phoenix.Component

  alias Logflare.Lql
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.LogEvent
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias LogflareWeb.SearchLive.LogEventComponents

  @default_attrs %{
    search_op_log_events: nil,
    log_events: [],
    last_query_completed_at: nil,
    loading: false,
    search_timezone: "Etc/UTC",
    tailing?: false,
    querystring: "",
    lql_rules: [],
    source: nil,
    search_op: nil
  }

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
          SchemaBuilder.initial_table_schema()
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
            search_op_log_events: search_op_log_events,
            log_events: stream_entries(search_op_log_events.rows)
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

      assert html =~ ~r|id="logs-list".*class="(.*)blurred"|
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
      log_event_without_message = %LogEvent{
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
            search_op_log_events: search_op_log_events,
            log_events: stream_entries(search_op_log_events.rows)
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

      log_event_without_message = %LogEvent{
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
            search_op_log_events: search_op_log_events,
            log_events: stream_entries(search_op_log_events.rows)
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
          food: "Pizza",
          deployment_time: 1_777_263_766_765_189
        )

      select_fields = [
        %{display: "metadata.user_id", key: "metadata_user_id", path: "metadata.user_id"},
        %{
          display: "metadata.store.city",
          key: "metadata_store_city",
          path: "metadata.store.city"
        },
        %{display: "food", key: "food", path: "food"},
        %{display: "deployment_time", key: "deployment_time", path: "deployment_time"}
      ]

      assigns = %{
        log_event: log_event,
        select_fields: select_fields,
        source_schema_flat_map: %{"deployment_time" => :datetime},
        timezone: "Australia/Brisbane"
      }

      html =
        rendered_to_string(~H"""
        <LogEventComponents.selected_fields
          log_event={@log_event}
          select_fields={@select_fields}
          source_schema_flat_map={@source_schema_flat_map}
          timezone={@timezone}
        />
        """)

      assert html =~ "user_id"
      assert html =~ "user_123"
      assert html =~ "city"
      assert html =~ "San Francisco"
      assert html =~ "food"
      assert html =~ "Pizza"

      {:ok, document} = Floki.parse_document(html)

      deployment_field =
        document
        |> Floki.find("div.tw-flex")
        |> Enum.find(&(Floki.text(&1) =~ "deployment_time"))

      assert Floki.text(deployment_field) =~ "1777263766765189"
      assert Floki.text(deployment_field) =~ "2026-04-27 14:22:46"

      assert Floki.attribute(deployment_field, "span[title]", "title") == [
               "2026-04-27T04:22:46Z"
             ]
    end

    test "does not render null values" do
      log_event = build(:log_event, metadata_user_id: nil)

      select_fields = [
        %{display: "metadata.user_id", key: "metadata_user_id", path: "metadata.user_id"}
      ]

      assigns = %{
        log_event: log_event,
        select_fields: select_fields
      }

      html =
        rendered_to_string(~H"""
        <LogEventComponents.selected_fields log_event={@log_event} select_fields={@select_fields} />
        """)

      refute html =~ "metadata.user_id"
      refute html =~ "null"
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
          long_field: String.duplicate("a", 80),
          plan_id: nil
        )

      lql_rules = [
        %SelectRule{path: "metadata.user_id", alias: nil},
        %SelectRule{path: "metadata.store.city", alias: "city"},
        %SelectRule{path: "long_field", alias: nil},
        %SelectRule{path: "metadata.plan_id", alias: nil}
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

             plan_id: null

             """
    end
  end

  defp stream_entries(log_events) do
    log_events
    |> Enum.with_index()
    |> Enum.map(fn {log_event, index} -> {"log-events-#{index}", log_event} end)
  end
end
