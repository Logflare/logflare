defmodule LogflareWeb.JSONViewerComponent do
  @moduledoc """
  Renders JSON data as an interactive, collapsible tree view.
  """
  use Phoenix.Component

  alias Phoenix.LiveView.JS

  @doc """
  ## Examples

      <.json_viewer data={%{name: "test", count: 42}} />
  """

  attr :data, :any, required: true, doc: "List or map to render as a tree"
  attr :class, :string, default: "", doc: "Additional CSS classes"
  attr :rest, :global, doc: "Global attributes"

  slot :action

  def json_viewer(assigns) do
    data =
      case assigns.data do
        list when is_list(list) ->
          Enum.with_index(list, fn v, index -> {to_string(index), v} end)

        map when is_map(map) ->
          map
      end

    assigns = Map.put(assigns, :data, data)

    ~H"""
    <div class={["tw-font-mono tw-text-sm", @class]} {@rest}>
      <.tree_node :for={{k, v} <- @data} key={k} value={v} path={[]} />
    </div>
    """
  end

  defp tree_node(%{value: _value, kind: _kind, path: path, key: key} = assigns) do
    full_path = path ++ [key]
    path_id = full_path |> Enum.map(&to_string/1) |> Enum.join("--")

    assigns =
      assigns
      |> Map.put(:full_path, full_path)
      |> Map.put(:path_id, path_id)

    ~H"""
    <div class="tw-my-0.5">
      <.disclosure_button target={@path_id} />
      <span class="tw-text-json-tree-key">{@key}:</span>
      <span class="tw-text-json-tree-label">{@kind}</span>

      <div class="tw-pl-4" id={@path_id}>
        <.tree_node_value value={@value} path={@full_path} />
      </div>
    </div>
    """
  end

  defp tree_node(%{value: value, path: _path} = assigns) when is_map(value) do
    assigns
    |> Map.put(:kind, "Object")
    |> tree_node()
  end

  defp tree_node(%{value: value, path: _path} = assigns) when is_list(value) do
    assigns
    |> Map.put(:kind, "Array")
    |> Map.put(:value, Enum.with_index(value, fn v, index -> {to_string(index), v} end))
    |> tree_node()
  end

  defp tree_node(%{path: path, key: key} = assigns) do
    full_path = path ++ [key]
    path_id = full_path |> Enum.map(&to_string/1) |> Enum.join("--")
    assigns = Map.put(assigns, :path_id, path_id)

    ~H"""
    <div class="tw-my-0.5 tw-overflow-hidden" id={@path_id}>
      <span class="tw-text-json-tree-key">{@key}:</span>
      <.tree_node_value value={@value} path={[]} />
    </div>
    """
  end

  defp tree_node_value(%{value: "http" <> _url} = assigns) do
    assigns = Map.put_new(assigns, :class, "tw-text-json-tree-string")

    ~H"""
    <span class={@class}>"</span><.link href={@value} target="_blank" class={@class}>{@value}</.link><span class={@class}>"</span>
    """
  end

  defp tree_node_value(%{value: value, class: _class} = assigns) when is_binary(value) do
    ~H"""
    <span class={@class}>{@value}</span>
    """
  end

  defp tree_node_value(%{value: value, path: _path}) when is_nil(value),
    do:
      tree_node_value(%{
        value: "null",
        class: "tw-text-json-tree-null"
      })

  defp tree_node_value(%{value: value, path: _path}) when is_number(value),
    do:
      tree_node_value(%{
        value: to_string(value),
        class: "tw-text-json-tree-number"
      })

  defp tree_node_value(%{value: value, path: _path}) when is_boolean(value),
    do:
      tree_node_value(%{
        value: to_string(value),
        class: "tw-text-json-tree-boolean"
      })

  defp tree_node_value(%{value: value, path: _path}) when is_binary(value),
    do:
      tree_node_value(%{
        value: ~s("#{value}"),
        class: "tw-text-json-tree-string"
      })

  defp tree_node_value(%{value: value, path: path} = assigns) when is_list(value) do
    assigns = Map.put(assigns, :path, path)

    ~H"""
    <.tree_node :for={{k, v} <- @value} value={v} key={k} path={@path} />
    """
  end

  defp tree_node_value(%{value: value, path: path} = assigns) when is_map(value) do
    assigns = Map.put(assigns, :path, path)

    ~H"""
    <.tree_node :for={{k, v} <- @value} value={v} key={k} path={@path} />
    """
  end

  attr :open, :boolean, default: true
  attr :target, :string, required: true

  def disclosure_button(assigns) do
    ~H"""
    <button
      type="button"
      phx-click={JS.toggle(to: "##{@target}") |> JS.toggle_class("tw-rotate-90")}
      class={[
        "tw-bg-transparent tw-border-none tw-p-0 tw-absolute -tw-translate-x-3 tw-translate-y-0.5 tw-inline-block tw-mr-1 tw-text-white tw-text-xs tw-select-none tw-cursor-pointer tw-transition-transform tw-duration-100",
        @open && "tw-rotate-90"
      ]}
    >
      ▶
    </button>
    """
  end
end
