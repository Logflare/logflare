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
    assert html =~ ~s(data-combobox-input-id="token_source_id-input")
    assert html =~ ~s(phx-hook="Combobox")
    assert html =~ ~s(<select id="token_source_id" name="token[source_id]")
    assert html =~ ~s(<option selected value="1">First</option>)
  end

  test "combobox forwards accessible label attributes to its native select" do
    html =
      render_component(&CoreComponents.combobox/1, %{
        id: "source",
        name: "source",
        value: nil,
        options: [{"First", "1"}],
        "aria-label": "Source",
        "aria-labelledby": "source-label",
        "aria-describedby": "source-help"
      })

    document = Floki.parse_fragment!(html)

    assert Floki.attribute(document, "#source", "aria-label") == ["Source"]
    assert Floki.attribute(document, "#source", "aria-labelledby") == ["source-label"]
    assert Floki.attribute(document, "#source", "aria-describedby") == ["source-help"]
  end
end
