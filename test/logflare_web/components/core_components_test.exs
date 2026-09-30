defmodule LogflareWeb.CoreComponentsTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.CoreComponents

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
end
