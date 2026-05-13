defmodule LogflareWeb.PaginationComponents do
  use Phoenix.Component

  @doc """
  Renders pagination links for a Scrivener.Page struct.

  ## Examples

      <.pagination_links page={@accounts} path={~p"/admin/accounts"} distance={3} />
  """
  attr :page, :map, required: true, doc: "%Scrivener.Page{} struct with pagination info"
  attr :path, :string, required: true, doc: "The base path for pagination links"

  attr :params, :list,
    default: [],
    doc: "Additional query params to include in links",
    examples: [[sort_by: "recent"]]

  attr :distance, :integer,
    default: 7,
    doc: "Number of page links to show on each side of current page"

  attr :first, :boolean, default: true, doc: "Show first page link"
  attr :last, :boolean, default: true, doc: "Show last page link"
  attr :next, :string, default: ">>", doc: "Text for next button"
  attr :previous, :string, default: "<<", doc: "Text for previous button"

  attr :patch, :boolean,
    default: false,
    doc: "Use patch navigation instead of href (for LiveView)"

  def pagination_links(assigns) do
    ~H"""
    <nav>
      <ul class="pagination">
        <%= for item <- Numerator.build(@page, num_pages_shown: @distance, show_first: @first, show_last: @last) do %>
          <.pagination_link_item href={build_url(@path, @params, item)} item={item} previous={@previous} next={@next} patch={@patch} />
        <% end %>
      </ul>
    </nav>
    """
  end

  attr :previous, :string
  attr :next, :string
  attr :item, :map
  attr :active, :any, default: false
  attr :href, :string
  attr :rel, :string
  attr :patch, :boolean, default: false
  slot :inner_block

  defp pagination_link_item(%{item: %{type: :ellipsis}} = assigns) do
    ~H"""
    <li class="page-item"><span class="page-link">&hellip;</span></li>
    """
  end

  defp pagination_link_item(%{item: %{type: :prev}} = assigns) do
    ~H"""
    <.pagination_link_item rel="prev" href={@href} patch={@patch}>{@previous}</.pagination_link_item>
    """
  end

  defp pagination_link_item(%{item: %{type: :next}} = assigns) do
    ~H"""
    <.pagination_link_item rel="next" href={@href} patch={@patch}>{@next}</.pagination_link_item>
    """
  end

  defp pagination_link_item(%{item: %{type: :page}} = assigns) do
    ~H"""
    <.pagination_link_item rel="canonical" href={@href} patch={@patch}>{@item.page}</.pagination_link_item>
    """
  end

  defp pagination_link_item(%{item: %{type: :current}} = assigns) do
    ~H"""
    <.pagination_link_item active="active" rel="canonical" href={@href} patch={@patch}>{@item.page}</.pagination_link_item>
    """
  end

  defp pagination_link_item(%{patch: true} = assigns) do
    ~H"""
    <li class={["page-item", @active]}><.link class="page-link" patch={@href} rel={@rel}>{render_slot(@inner_block)}</.link></li>
    """
  end

  defp pagination_link_item(assigns) do
    ~H"""
    <li class={["page-item", @active]}><.link class="page-link" href={@href} rel={@rel}>{render_slot(@inner_block)}</.link></li>
    """
  end

  defp build_url(path, params, %{page: page}) when is_integer(page) do
    query_params =
      if page > 1 do
        params ++ [page: page]
      else
        params
      end
      |> URI.encode_query()

    if query_params == "" do
      path
    else
      path <> "?" <> query_params
    end
  end

  defp build_url(_path, _params, _), do: nil
end
