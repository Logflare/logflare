defmodule LogflareWeb.SearchLive.LogEventComponentsTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.LiveViewTest
  import Phoenix.Component

  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.LogEvent
  alias Logflare.Logs.SearchOperation
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias LogflareWeb.SearchLive.LogEventComponents

  @default_attrs %{
    search_op_log_events: nil,
    log_events: [],
    loading: false,
    pagination_buttons: %{
      previous: %{state: :hidden, cursor: nil},
      next: %{state: :hidden, cursor: nil}
    },
    search_timezone: "Etc/UTC",
    tailing?: false
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

      {:ok, lql_rules} =
        Lql.decode(
          "c:count(*) c:group_by(t::minute)",
          SchemaBuilder.initial_table_schema()
        )

      search_op_log_events =
        search_operation(source, lql_rules, [
          build(:log_event, message: "Log message 1", metadata: %{user_id: 123}, source: source)
        ])

      [
        source: source,
        search_op_log_events: search_op_log_events,
        lql_rules: lql_rules
      ]
    end

    test "renders log events list", %{search_op_log_events: search_op_log_events} do
      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op_log_events: search_op_log_events,
            log_events: stream_entries(search_op_log_events.rows)
        })

      assert html =~ "Log message 1"
      assert html =~ ~s(data-tailing="false")
    end

    test "renders loading state", %{search_op_log_events: search_op_log_events} do
      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | loading: true,
            search_op_log_events: %{search_op_log_events | rows: []}
        })

      assert html =~ ~r|id="logs-list".*class="(.*)blurred"|
    end

    test "renders the previous-page pagination button", %{
      source: source,
      search_op_log_events: search_op_log_events,
      lql_rules: lql_rules
    } do
      search_op =
        pagination_search_op(
          source,
          lql_rules,
          [~N[2026-01-01 00:00:00], ~N[2026-01-01 01:00:00]]
        )

      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op_log_events: search_op,
            log_events: stream_entries(search_op_log_events.rows),
            pagination_buttons: %{
              previous: %{state: :ready, cursor: %{id: "previous", timestamp: 1}},
              next: %{state: :hidden, cursor: nil}
            }
        })

      assert html =~ ~s(id="load-more-events-top")
      assert html =~ ~s(phx-value-intent="previous")
      assert html =~ ~s(phx-value-cursor-id="previous")
      assert html =~ ~s(phx-value-cursor-timestamp="1")

      [label] =
        html
        |> Floki.parse_fragment!()
        |> Floki.find("#load-more-events-top span")
        |> Enum.filter(&(Floki.text(&1) == "Load more"))

      refute label
             |> Floki.attribute("class")
             |> Enum.join(" ")
             |> String.split()
             |> Enum.member?("phx-click-loading")
    end

    test "keeps the previous-page button mounted and hidden without a sentinel cursor", %{
      search_op_log_events: search_op_log_events
    } do
      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op_log_events: search_op_log_events,
            log_events: stream_entries(search_op_log_events.rows),
            pagination_buttons: %{
              previous: %{state: :hidden, cursor: nil},
              next: %{state: :hidden, cursor: nil}
            }
        })

      document = Floki.parse_fragment!(html)

      assert [_button] =
               Floki.find(document, "div.tw-hidden > #load-more-events-top[disabled]")

      assert [_button] =
               Floki.find(document, "div.tw-hidden > #load-more-events-bottom[disabled]")
    end

    test "disables pagination buttons while the search is loading", %{
      source: source,
      search_op_log_events: search_op_log_events,
      lql_rules: lql_rules
    } do
      search_op =
        pagination_search_op(
          source,
          lql_rules,
          [~N[2026-01-01 00:00:00], ~N[2026-01-01 01:00:00]]
        )

      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op_log_events: search_op,
            log_events: stream_entries(search_op_log_events.rows),
            loading: true,
            pagination_buttons: %{
              previous: %{state: :disabled, cursor: %{id: "previous", timestamp: 1}},
              next: %{state: :disabled, cursor: %{id: "next", timestamp: 2}}
            }
        })

      document = Floki.parse_fragment!(html)

      assert [_top_button] =
               Floki.find(
                 document,
                 "#load-more-events-top[disabled][phx-click='load_events']"
               )

      assert [_bottom_button] =
               Floki.find(
                 document,
                 "#load-more-events-bottom[disabled][phx-click='load_events'][phx-value-intent='next']"
               )
    end

    test "renders empty state when no log events" do
      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | loading: false,
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

      search_op_log_events = search_operation(source, lql_rules, [log_event_without_message])

      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op_log_events: search_op_log_events,
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

      search_op_log_events =
        search_operation(source, lql_rules, [normal_log_event, log_event_without_message])

      html =
        render_component(&LogEventComponents.results_list/1, %{
          @default_attrs
          | search_op_log_events: search_op_log_events,
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

  defp search_operation(source, lql_rules, rows) do
    %SearchOperation{
      chart_data_shape_id: nil,
      partition_by: :timestamp,
      querystring: "",
      rows: rows,
      source: source,
      lql_rules: lql_rules,
      search_timezone: "Etc/UTC",
      tailing?: false
    }
  end

  defp pagination_search_op(source, lql_rules, timestamps) do
    %SearchOperation{
      chart_data_shape_id: nil,
      partition_by: :timestamp,
      querystring: "",
      source: source,
      lql_rules: lql_rules,
      lql_ts_filters: [
        %FilterRule{path: "timestamp", operator: :range, values: timestamps}
      ],
      search_timezone: "Etc/UTC",
      tailing?: false
    }
  end
end
