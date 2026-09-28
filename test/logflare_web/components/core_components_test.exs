defmodule LogflareWeb.CoreComponentsTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.CoreComponents

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
