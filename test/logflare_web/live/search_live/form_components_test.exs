defmodule LogflareWeb.SearchLive.FormComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias Logflare.Sources.Source
  alias Logflare.User
  alias LogflareWeb.SearchLive.AiAssist
  alias LogflareWeb.SearchLive.FormComponents

  describe "search_controls/1" do
    test "shows the platform shortcut when AI assist is enabled" do
      for {macintosh?, shortcut} <- [{true, "⌘ Enter"}, {false, "Ctrl+Enter"}] do
        assert [_button] =
                 %{ai_assist: %AiAssist{enabled?: true, macintosh?: macintosh?}}
                 |> render_search_controls()
                 |> Floki.find(~s|#ai-search-button[data-title*="#{shortcut}"]|)
      end
    end

    test "hides AI assist when disabled" do
      document = render_search_controls(%{ai_assist: %AiAssist{}})
      assert Floki.find(document, "#ai-search-button") == []
    end

    test "renders one-time bad response feedback after an AI search" do
      feedback = %{
        natural_language_request: "stores in Phoenix",
        anthropic_request_id: "request-123",
        submitted?: false
      }

      document =
        render_search_controls(%{
          querystring: "m.store.city:Phoenix",
          ai_assist: %AiAssist{enabled?: true, feedback: feedback}
        })

      assert [link] = Floki.find(document, "#ai-feedback-menu > button:not([disabled])")
      assert Floki.text(link) =~ "Send feedback"

      assert [option] = Floki.find(document, "#ai-poor-response-feedback")
      assert Floki.text(option) =~ "Poor response"
      assert [_icon] = Floki.find(option, ".far.fa-thumbs-down")

      assert [
               ["push", %{"event" => "submit_ai_feedback"}],
               ["hide", %{"to" => "#ai-feedback-menu ul"}]
             ] =
               option
               |> Floki.attribute("phx-click")
               |> List.first()
               |> JSON.decode!()

      assert [_menu] = Floki.find(document, "div.ml-auto.pb-1 > #ai-feedback-menu")
      assert Floki.find(document, ".lql-editor-wrapper #ai-feedback-menu") == []

      submitted_document =
        render_search_controls(%{
          querystring: "m.store.city:Phoenix",
          ai_assist: %AiAssist{enabled?: true, feedback: %{feedback | submitted?: true}}
        })

      assert [link] = Floki.find(submitted_document, "#ai-feedback-menu > button[disabled]")
      assert Floki.text(link) =~ "Feedback sent."
    end
  end

  describe "recommended_field_inputs/1" do
    test "dedupes fields, preserves first-seen order, and merges required flag" do
      html =
        render_component(&FormComponents.recommended_field_inputs/1, %{
          fields: [" session_id ", "metadata.level", "metadata.level!", "", "event_message"],
          id_prefix: "search-field"
        })

      document = Floki.parse_document!(html)

      field_blocks = Floki.find(document, "div.recommended-field-container")

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

  defp render_search_controls(overrides) do
    assigns =
      Map.merge(
        %{
          querystring: "",
          saved_searches: [],
          loading: false,
          tailing?: false,
          uri_params: %{},
          lql_rules: [],
          user: %User{id: 1},
          has_results?: false,
          source: %Source{id: 1},
          lql_schema_flat_map: %{},
          ai_assist: %AiAssist{}
        },
        overrides
      )

    render_component(&FormComponents.search_controls/1, assigns)
    |> Floki.parse_document!()
  end
end
