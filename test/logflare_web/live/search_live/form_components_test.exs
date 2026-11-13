defmodule LogflareWeb.SearchLive.FormComponentsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Sources.Source
  alias Logflare.User
  alias LogflareWeb.SearchLive.AiAssist
  alias LogflareWeb.SearchLive.FormComponents

  describe "chart_controls/1" do
    test "hides the aggregate label and dropdown only for multiple chart rules" do
      sum = %ChartRule{aggregate: :sum, path: "metadata.latency"}
      avg = %ChartRule{aggregate: :avg, path: "metadata.latency"}

      for rules <- [[], [sum], [sum, avg]] do
        document =
          render_component(&FormComponents.chart_controls/1, %{
            lql_rules: rules,
            chart_aggregate_enabled?: rules != []
          })
          |> Floki.parse_document!()

        assert [_period] = Floki.find(document, "#search_chart_period")

        if length(rules) > 1 do
          assert Floki.find(document, "#search_chart_aggregate") == []
          refute Floki.text(document) =~ "Aggregate:"
        else
          assert [_aggregate] = Floki.find(document, "#search_chart_aggregate")
          assert Floki.text(document) =~ "Aggregate:"
        end
      end
    end
  end

  describe "search_controls/1" do
    test "shows the platform shortcut when AI assist is enabled" do
      for {macintosh?, shortcut} <- [{true, "⌘ Enter"}, {false, "Ctrl+Enter"}] do
        document =
          render_search_controls(%{ai_assist: %AiAssist{enabled?: true, macintosh?: macintosh?}})

        assert [button] = Floki.find(document, "#ai-search-button")

        assert Floki.attribute(button, "disabled") == []
        assert Floki.attribute(button, "data-title") == ["AI search assistant<br>#{shortcut}"]

        assert Floki.find(document, "#ai-search-upgrade-prompt") == []
      end
    end

    test "hides AI assist when disabled" do
      document = render_search_controls(%{ai_assist: %AiAssist{}})
      assert Floki.find(document, "#ai-search-button") == []
    end

    test "shows disabled AI assist button with upgrade prompt" do
      document =
        render_search_controls(%{
          ai_assist: %AiAssist{upgrade_required?: true, loading?: true},
          team: %Logflare.Teams.Team{id: 42}
        })

      assert [_button] = Floki.find(document, "#ai-search-button[disabled]")

      prompt =
        document
        |> Floki.find("#ai-search-upgrade-prompt")
        |> Floki.attribute("data-title")
        |> hd()
        |> Floki.parse_fragment!()

      assert [link] =
               Enum.filter(Floki.find(prompt, "a"), fn link ->
                 link |> Floki.text() |> String.trim() == "Upgrade your plan"
               end)

      assert Floki.attribute(link, "href") == ["/billing/edit?t=42"]
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
