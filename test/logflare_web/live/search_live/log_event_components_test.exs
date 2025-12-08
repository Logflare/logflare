defmodule LogflareWeb.SearchLive.LogEventComponentsTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.LiveViewTest
  import Phoenix.Component

  alias LogflareWeb.SearchLive.LogEventComponents

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

      assert html =~ "metadata.user_id:"
      assert html =~ "user_123"
      assert html =~ "metadata.store.city:"
      assert html =~ "San Francisco"
      assert html =~ "food:"
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

      assert html =~ "metadata.user_id:"
      assert html =~ "null"
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

             metadata.user_id: user_123

             city: San Francisco

             long_field:
             aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

             """
    end
  end
end
