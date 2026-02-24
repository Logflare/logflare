defmodule LogflareWeb.SearchLive.FormComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias LogflareWeb.SearchLive.FormComponents

  describe "recommended_field_inputs/1" do
    test "dedupes fields, preserves first-seen order, and merges required flag" do
      html =
        render_component(&FormComponents.recommended_field_inputs/1, %{
          fields: [" session_id ", "metadata.level", "metadata.level!", "", "event_message"],
          id_prefix: "search-field"
        })

      document = Floki.parse_document!(html)

      field_blocks = Floki.find(document, "div.pr-2.pt-1.pb-1")

      fields =
        Enum.map(field_blocks, fn block ->
          label = block |> Floki.find("label") |> Floki.text()
          required_indicators = block |> Floki.find(".required-field-indicator") |> length()

          {label, required_indicators}
        end)

      assert fields == [{"session_id", 0}, {"metadata.level", 1}, {"event_message", 0}]
    end

    test "renders nothing when all fields resolve to empty names" do
      html =
        render_component(&FormComponents.recommended_field_inputs/1, %{
          fields: ["", "   ", "!"],
          id_prefix: "search-field"
        })

      refute html =~ "search-field["
      assert html |> Floki.parse_document!() |> Floki.find("input") == []
    end
  end
end
