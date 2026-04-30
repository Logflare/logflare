defmodule LogflareWeb.FormattedTimestampComponentTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.LiveViewTest
  import LogflareWeb.FormattedTimestampComponent

  describe "formatted_timestamp/1" do
    test "renders formatted timestamp with ISO8601 tooltip" do
      timestamp = 1_777_263_766_765_189

      html =
        render_component(&formatted_timestamp/1,
          value: timestamp,
          timezone: "Etc/UTC"
        )

      assert html =~ "2026-04-27 04:22:46"
      assert html =~ ~s(title="2026-04-27T04:22:46Z")
    end

    test "renders search timezone adjusted timestamp without timezone suffix" do
      timestamp = 1_777_263_766_765_189

      html =
        render_component(&formatted_timestamp/1,
          value: timestamp,
          timezone: "Australia/Brisbane"
        )

      assert html |> Floki.parse_document!() |> Floki.text() |> String.trim() ==
               "2026-04-27 14:22:46"

      assert html =~ ~s(title="2026-04-27T04:22:46Z")
    end

    test "renders explicit UTC suffix when no timezone is active" do
      timestamp = 1_777_263_766_765_189

      html =
        render_component(&formatted_timestamp/1,
          value: timestamp
        )

      assert html =~ "2026-04-27 04:22:46 UTC"
      assert html =~ ~s(title="2026-04-27T04:22:46Z")
    end

    test "does not render when the value is not parseable as a timestamp" do
      ["not-a-timestamp", nil]
      |> Enum.each(fn bad_value ->
        html = render_component(&formatted_timestamp/1, value: bad_value)
        assert html == ""
      end)
    end
  end
end
