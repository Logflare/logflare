defmodule LogflareWeb.ItemListComponents do
  @moduledoc """
  Renders a list of user-owned items with a uniform row layout.

  The `:item` slot is rendered once per item and receives the item as its slot
  argument. An optional `:empty` slot replaces the default empty state.

  ## Examples

      <.item_list id="source-list" items={@sources} title="Sources">
        <:item :let={source}>{source.name}</:item>
        <:empty>No sources yet.</:empty>
      </.item_list>
  """

  use Phoenix.Component

  attr :id, :string, required: true
  attr :items, :list, required: true
  attr :title, :string, default: nil
  attr :class, :any, default: nil
  attr :row_class, :any, default: "tw-flex tw-items-center tw-justify-between tw-py-2"
  attr :row_id, :fun, default: nil, doc: "Derives the DOM id of each row from its item."
  attr :empty_text, :string, default: "Nothing here yet."

  slot :item, required: true, doc: "Row content, receives the item as its slot argument."
  slot :empty, doc: "Replaces the default empty state."

  @spec item_list(map()) :: Phoenix.LiveView.Rendered.t()
  def item_list(assigns) do
    ~H"""
    <div class={@class}>
      <h5 :if={@title} class="header-margin">{@title}</h5>
      <ul :if={@items != []} id={@id} class="list-unstyled tw-mb-0">
        <li :for={item <- @items} id={row_id(@row_id, item)} class={@row_class}>
          {render_slot(@item, item)}
        </li>
      </ul>
      <div :if={@items == []}>
        <div :if={@empty == []} class="tw-flex tw-items-center tw-justify-center tw-text-center tw-text-gray-500 tw-text-sm">
          {@empty_text}
        </div>
        {render_slot(@empty)}
      </div>
    </div>
    """
  end

  defp row_id(nil, _item), do: nil

  defp row_id(fun, item), do: fun.(item)
end
