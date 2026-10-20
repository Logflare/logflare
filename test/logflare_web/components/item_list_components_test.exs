defmodule LogflareWeb.ItemListComponentsTest do
  use LogflareWeb.ConnCase, async: true

  import Phoenix.Component
  import Phoenix.LiveViewTest

  alias LogflareWeb.ItemListComponents

  @spec item_list_sample(map()) :: Phoenix.LiveView.Rendered.t()
  def item_list_sample(assigns) do
    ~H"""
    <ItemListComponents.item_list id="sample-list" items={@items} row_id={&"item-#{&1.id}"}>
      <:empty :if={@custom_empty?}>
        <p id="custom-empty">custom empty</p>
      </:empty>
      <:item :let={item}>{item.name}</:item>
    </ItemListComponents.item_list>
    """
  end

  test "renders items with row ids" do
    rendered =
      render_component(&item_list_sample/1,
        items: [%{id: 3, name: "first"}, %{id: 7, name: "second"}],
        custom_empty?: false
      )
      |> Floki.parse_fragment!()

    assert Floki.find(rendered, "#sample-list li") |> Floki.attribute("id") == [
             "item-3",
             "item-7"
           ]

    assert Floki.find(rendered, "#item-3") |> Floki.text() |> String.trim() == "first"
    assert Floki.find(rendered, "#item-7") |> Floki.text() |> String.trim() == "second"
  end

  test "renders the default empty state or the empty slot" do
    for custom_empty? <- [false, true] do
      rendered = render_component(&item_list_sample/1, items: [], custom_empty?: custom_empty?)
      expected = if custom_empty?, do: "custom empty", else: "Nothing here yet."
      assert rendered |> Floki.parse_fragment!() |> Floki.text() |> String.trim() == expected
      refute rendered =~ "sample-list"
    end
  end
end
