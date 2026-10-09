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

    test "renders DateTime and UTC NaiveDateTime values in the selected timezone" do
      for timestamp <- [
            ~N[2026-10-08 18:24:56.123456],
            ~U[2026-10-08 18:24:56.123456Z],
            Timex.Timezone.convert(~U[2026-10-08 18:24:56.123456Z], "America/New_York")
          ] do
        html =
          render_component(&formatted_timestamp/1,
            value: timestamp,
            timezone: "Australia/Brisbane",
            class: "tw-text-sm"
          )

        assert html |> Floki.parse_fragment!() |> Floki.text() |> String.trim() ==
                 "2026-10-09 04:24:56"

        assert html =~ ~s(title="2026-10-08T18:24:56Z")
        assert html =~ ~s(class="tw-text-sm")

        for timezone <- [nil, "invalid/timezone"] do
          html = render_component(&formatted_timestamp/1, value: timestamp, timezone: timezone)
          assert html =~ "2026-10-08 18:24:56 UTC"
          assert html =~ ~s(title="2026-10-08T18:24:56Z")
        end
      end
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
