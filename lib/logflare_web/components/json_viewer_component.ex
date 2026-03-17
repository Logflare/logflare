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
  attr :id, :string, default: nil, doc: "Optional prefix for DOM ids"
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

    assigns = assign(assigns, :data, data)

    ~H"""
    <div id={@id} class={["tw-font-mono tw-text-sm", @class]} {@rest}>
      <.tree_node :for={{k, v} <- @data} key={k} label={k} value={v} path={[]} key_path={[]} id={@id} action={@action} />
    </div>
    """
  end

  defp tree_node(%{value: _value, kind: _kind, path: path, key: _key} = assigns) do
    full_path = append_path(path, assigns.label)
    path_id = build_path_id(full_path, assigns.id)

    assigns =
      assigns
      |> assign(
        path: full_path,
        key_path: append_path(assigns.key_path, assigns.key),
        path_id: path_id
      )
      |> assign_new(:children, fn -> nil end)
      |> assign_new(:action, fn -> [] end)

    ~H"""
    <div class="tw-my-0.5">
      <.disclosure_button target={@path_id} />
      <span class="tw-text-json-tree-key">{@key}:</span>
      <span class="tw-text-json-tree-label">{@kind}</span>

      <div class="tw-pl-4" id={@path_id}>
        <.tree_node_value value={@value} children={@children} key={@key} path={@path} key_path={@key_path} id={@id} action={@action} />
      </div>
    </div>
    """
  end

  defp tree_node(%{value: value, path: _path} = assigns) when is_map(value) do
    assigns
    |> assign(kind: "Object", children: value)
    |> tree_node()
  end

  defp tree_node(%{value: value, path: _path} = assigns) when is_list(value) do
    assigns
    |> assign(
      kind: ["Array", " (", to_string(length(value)), ")"],
      children: Enum.with_index(value, fn v, index -> {to_string(index), v} end)
    )
    |> tree_node()
  end

  defp tree_node(%{path: path, key_path: key_path} = assigns) do
    path = append_path(path, assigns.label)
    key_path = append_path(key_path, assigns.key)

    assigns =
      assigns
      |> assign(path: path, key_path: key_path)
      |> assign_new(:children, fn -> nil end)
      |> assign_new(:action, fn -> [] end)

    ~H"""
    <div class="tw-my-0.5 tw-overflow-hidden tw-group" id={build_path_id(@path, @id)}>
      <span class="tw-text-json-tree-key">{@label}:</span>
      <.tree_node_value key={@key} value={@value} children={@children} path={@path} key_path={@key_path} id={@id} action={@action} />
    </div>
    """
  end

  defp tree_node_value(%{value: "http" <> _url} = assigns) do
    assigns =
      assigns
      |> assign(class: "tw-text-json-tree-string")

    ~H"""
    <span class={@class}>"</span><.link href={@value} target="_blank" class={@class}>{@value}</.link><span class={@class}>"</span>
    {render_slot(@action, %{key: @key, value: @value, path: @key_path})}
    """
  end

  defp tree_node_value(%{formatted_value: _, class: _class} = assigns) do
    ~H"""
    <span class={@class}>
      {@formatted_value}
      {render_slot(@action, %{key: @key, value: @value, path: @key_path})}
    </span>
    """
  end

  defp tree_node_value(%{value: value, path: _path} = assigns) when is_nil(value) do
    assigns
    |> assign(
      formatted_value: "null",
      class: "tw-text-json-tree-null"
    )
    |> tree_node_value()
  end

  defp tree_node_value(%{value: value, path: _path} = assigns) when is_number(value) do
    assigns
    |> assign(
      formatted_value: to_string(value),
      class: "tw-text-json-tree-number"
    )
    |> tree_node_value()
  end

  defp tree_node_value(%{value: value, path: _path} = assigns) when is_boolean(value) do
    assigns
    |> assign(
      formatted_value: to_string(value),
      class: "tw-text-json-tree-boolean"
    )
    |> tree_node_value()
  end

  defp tree_node_value(%{value: value, path: _path} = assigns) when is_binary(value) do
    assigns
    |> assign(
      formatted_value: ["\"", value, "\""],
      class: "tw-text-json-tree-string"
    )
    |> tree_node_value()
  end

  defp tree_node_value(%{children: children, path: _path, key_path: _key_path} = assigns)
       when is_list(children) do
    ~H"""
    <.tree_node :for={{index, v} <- @children} key={nil} label={index} value={v} path={@path} key_path={@key_path} id={@id} action={@action} />
    """
  end

  defp tree_node_value(%{children: children, path: _path, key_path: _key_path} = assigns)
       when is_map(children) do
    ~H"""
    <.tree_node :for={{k, v} <- @children} value={v} key={k} label={k} path={@path} key_path={@key_path} id={@id} action={@action} />
    """
  end

  defp append_path(path, nil), do: path
  defp append_path(path, segment), do: path ++ [segment]

  defp build_path_id(path, root_id) do
    segments =
      path
      |> Enum.map(&normalize_path_segment/1)
      |> Enum.intersperse("--")

    if is_binary(root_id) and root_id != "" do
      [root_id, "--", segments]
    else
      segments
    end
  end

  defp normalize_path_segment(segment) do
    segment
    |> to_string()
    |> String.replace(~r/[^a-zA-Z0-9_-]/, "_")
  end

  attr :open, :boolean, default: true
  attr :target, :string, required: true

  defp disclosure_button(assigns) do
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
