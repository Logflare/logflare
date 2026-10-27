defmodule LogflareWeb.CoreComponentsTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.Component

  alias LogflareWeb.CoreComponents
  alias Phoenix.LiveView.JS

  test "tabs render results and loading plan states and compose optional commands" do
    for plan_active? <- [false, true] do
      assigns = %{plan_active?: plan_active?}

      html =
        rendered_to_string(~H"""
        <CoreComponents.tabs id="query-tabs" aria-label="Query output">
          <:tab id="results" label="Results" active={not @plan_active?}>Rows</:tab>
          <:tab id="plan" label="Query plan" active={@plan_active?} busy={@plan_active?} on_select={JS.push("load-plan")}>
            {if @plan_active?, do: "Loading plan", else: "Plan"}
          </:tab>
        </CoreComponents.tabs>
        """)
        |> Floki.parse_fragment!()

      assert [_] = Floki.find(html, ~s(#query-tabs[role="tablist"][aria-label="Query output"]))
      active_id = if plan_active?, do: "plan", else: "results"
      assert Floki.attribute(html, "#query-tabs .active", "id") == ["#{active_id}-tab"]
      assert Floki.attribute(html, "#query-tabs-panels .active", "id") == [active_id]
      assert Floki.text(Floki.find(html, "#results")) =~ "Rows"

      assert Floki.text(Floki.find(html, "#plan")) =~
               if(plan_active?, do: "Loading plan", else: "Plan")

      assert Floki.attribute(html, "#plan", "aria-busy") == [to_string(plan_active?)]

      for {id, label, active?} <- [
            {"results", "Results", not plan_active?},
            {"plan", "Query plan", plan_active?}
          ] do
        assert [button] =
                 Floki.find(
                   html,
                   ~s(#query-tabs button##{id}-tab[type="button"][role="tab"][aria-controls="#{id}"][aria-selected="#{active?}"])
                 )

        assert Floki.text(button) =~ label

        assert [_] =
                 Floki.find(
                   html,
                   ~s(#query-tabs-panels > ##{id}[role="tabpanel"][aria-labelledby="#{id}-tab"][tabindex="0"])
                 )

        commands = button |> Floki.attribute("phx-click") |> hd() |> Jason.decode!()

        assert Enum.any?(commands, fn
                 ["add_class", %{"to" => target, "names" => ["active"]}] -> target == "##{id}"
                 _ -> false
               end)

        pushes = Enum.filter(commands, fn [command, _opts] -> command == "push" end)

        if id == "plan" do
          assert [["push", %{"event" => "load-plan"}]] = pushes
          assert List.last(commands) == hd(pushes)
        else
          assert pushes == []
        end
      end
    end
  end

  test "button defaults to a non-submit button and accepts a submit type" do
    inner_block = [%{inner_block: fn _, _ -> "Run query" end}]

    default =
      render_component(&CoreComponents.button/1, %{
        variant: "secondary",
        inner_block: inner_block
      })

    submit =
      render_component(&CoreComponents.button/1, %{
        variant: "secondary",
        type: "submit",
        disabled: true,
        inner_block: inner_block
      })

    assert default =~ ~s(type="button")
    assert submit =~ ~s(type="submit")
    assert submit =~ "disabled"
  end

  test "input derives id, name, and value from a form field and accepts overrides" do
    form = Phoenix.Component.to_form(%{"description" => "Original"}, as: :token)

    html = render_component(&CoreComponents.input/1, %{field: form[:description]})

    assert html =~
             ~s(type="text" id="token_description" name="token[description]" value="Original")

    html =
      render_component(&CoreComponents.input/1, %{
        field: form[:description],
        id: "custom",
        name: "description",
        value: "Override",
        type: "search"
      })

    assert html =~ ~s(type="search" id="custom" name="description" value="Override")
    refute html =~ "<label"
  end

  test "radio input preserves explicit option values and checked states" do
    form = Phoenix.Component.to_form(%{"mode" => "selected"}, as: :permission)

    for {value, checked} <- [{"all", false}, {"selected", true}] do
      html =
        render_component(&CoreComponents.input/1, %{
          field: form[:mode],
          type: "radio",
          id: "mode-#{value}",
          value: value,
          checked: checked
        })
        |> Floki.parse_fragment!()

      assert [_] =
               Floki.find(
                 html,
                 ~s(input[type="radio"][name="permission[mode]"][value="#{value}"])
               )

      assert Floki.find(html, "input[checked]") != [] == checked
      assert Floki.find(html, "input[type='hidden']") == []
    end
  end

  test "checkbox input normalizes field values and submits false when unchecked" do
    for value <- [true, "true", false, "false", nil] do
      form = Phoenix.Component.to_form(%{"private" => value}, as: :token)

      html =
        render_component(&CoreComponents.input/1, %{
          field: form[:private],
          type: "checkbox"
        })
        |> Floki.parse_fragment!()

      assert [_] =
               Floki.find(html, ~s(input[type="hidden"][name="token[private]"][value="false"]))

      assert [_] =
               Floki.find(
                 html,
                 ~s(input[type="checkbox"]#token_private[name="token[private]"][value="true"])
               )

      assert Floki.find(html, "label, div") == []
      assert Floki.find(html, "input[checked]") != [] == value in [true, "true"]
    end
  end

  test "checkbox input allows overriding checked state and forwards disabled and form" do
    form = Phoenix.Component.to_form(%{"private" => true}, as: :token)

    html =
      render_component(&CoreComponents.input/1, %{
        field: form[:private],
        type: "checkbox",
        checked: false,
        disabled: true,
        form: "external-form"
      })
      |> Floki.parse_fragment!()

    assert Floki.find(html, "input[checked]") == []
    assert length(Floki.find(html, ~s(input[disabled][form="external-form"]))) == 2
  end

  test "select derives id, name, and value from a form field" do
    form = Phoenix.Component.to_form(%{"source_id" => "2"}, as: :token)

    html =
      render_component(&CoreComponents.select/1, %{
        field: form[:source_id],
        options: [{"First", "1"}, {"Second", "2"}]
      })

    assert html =~ ~s(<select id="token_source_id" name="token[source_id]")
    assert html =~ ~s(<option selected value="2">Second</option>)
  end

  test "combobox derives id, name, and value from a form field" do
    form = Phoenix.Component.to_form(%{"source_id" => "1"}, as: :token)

    html =
      render_component(&CoreComponents.combobox/1, %{
        field: form[:source_id],
        options: [{"First", "1"}]
      })

    assert html =~ ~s(id="token_source_id-combobox")
    assert html =~ ~s(phx-hook="Combobox")
    assert html =~ ~s(<select id="token_source_id" name="token[source_id]")
    assert html =~ ~s(<option selected value="1">First</option>)
  end

  test "combobox renders a native multiple select with all selected values" do
    html =
      render_component(&CoreComponents.combobox/1, %{
        id: "sources",
        name: "sources[]",
        value: ["1", "3"],
        options: [{"First", "1"}, {"Second", "2"}, {"Third", "3"}],
        multiple: true
      })

    assert html =~ ~s(<select id="sources" name="sources[]")
    assert html =~ ~s(class="" multiple)
    assert html =~ ~s(<option selected value="1">First</option>)
    refute html =~ ~s(<option selected value="2">Second</option>)
    assert html =~ ~s(<option selected value="3">Third</option>)
  end
end
