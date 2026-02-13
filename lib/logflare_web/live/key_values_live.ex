defmodule LogflareWeb.KeyValuesLive do
  @moduledoc false
  use LogflareWeb, :live_view

  alias Logflare.Billing
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache
  alias Logflare.Repo

  @page_size 500

  def mount(_params, _session, socket) do
    %{assigns: %{user: user}} = socket

    socket =
      socket
      |> assign(:total_count, Cache.count(user.id))
      |> assign(:page, nil)
      |> assign(:searched?, false)
      |> assign(:filter_key, "")
      |> assign(:show_create_form, false)

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    key = params["key"]

    socket =
      if non_blank?(key) do
        page_num = String.to_integer(params["page"] || "1")
        query = KeyValues.list_key_values_query(user_id: socket.assigns.user.id, key: key)

        socket
        |> assign(:page, paginate(query, page_num))
        |> assign(:searched?, true)
        |> assign(:filter_key, key || "")
      else
        socket
        |> assign(:page, nil)
        |> assign(:searched?, false)
        |> assign(:filter_key, "")
      end

    {:noreply, socket}
  end

  def handle_event("search", params, socket) do
    key = params["key"]

    if non_blank?(key) do
      {:noreply, push_patch(socket, to: ~p"/key-values?#{%{"key" => key}}")}
    else
      {:noreply, put_flash(socket, :error, "Key filter is required")}
    end
  end

  def handle_event("create", params, socket) do
    user = socket.assigns.user
    plan = Billing.get_plan_by_user(user)
    current_count = KeyValues.count_key_values(user.id)

    if current_count >= plan.limit_key_values do
      {:noreply, put_flash(socket, :error, "Key-value limit of #{plan.limit_key_values} reached")}
    else
      case parse_value_input(params["value"]) do
        {:error, msg} ->
          {:noreply, put_flash(socket, :error, msg)}

        {:ok, value} ->
          create_key_value(socket, params["key"], value)
      end
    end
  end

  def handle_event("delete", %{"id" => id}, socket) do
    user = socket.assigns.user
    kv = KeyValues.get_key_value(id)

    if kv && kv.user_id == user.id do
      {:ok, _} = KeyValues.delete_key_value(kv)

      socket =
        socket
        |> assign(:total_count, KeyValues.count_key_values(user.id))
        |> refresh_search()
        |> put_flash(:info, "Key-value pair deleted")

      {:noreply, socket}
    else
      {:noreply, put_flash(socket, :error, "Key-value pair not found")}
    end
  end

  def handle_event("toggle-create-form", _params, socket) do
    {:noreply, assign(socket, :show_create_form, !socket.assigns.show_create_form)}
  end

  def handle_event("clear-search", _params, socket) do
    {:noreply, push_patch(socket, to: ~p"/key-values")}
  end

  defp create_key_value(socket, key, value) do
    user = socket.assigns.user

    case KeyValues.create_key_value(%{user_id: user.id, key: key, value: value}) do
      {:ok, _kv} ->
        socket =
          socket
          |> assign(:total_count, KeyValues.count_key_values(user.id))
          |> assign(:show_create_form, false)
          |> put_flash(:info, "Key-value pair created")

        {:noreply, socket}

      {:error, changeset} ->
        msg = LogflareWeb.Utils.stringify_changeset_errors(changeset)
        {:noreply, put_flash(socket, :error, msg)}
    end
  end

  defp refresh_search(socket) do
    if socket.assigns.searched? do
      query_opts =
        [user_id: socket.assigns.user.id]
        |> maybe_put(:key, socket.assigns.filter_key)

      query = KeyValues.list_key_values_query(query_opts)
      assign(socket, :page, paginate(query, 1))
    else
      socket
    end
  end

  defp paginate(query, page_num) do
    import Ecto.Query, only: [offset: 2, limit: 2]

    total = Repo.aggregate(query, :count)
    total_pages = max(ceil(total / @page_size), 1)
    offset = (page_num - 1) * @page_size

    entries =
      query
      |> offset(^offset)
      |> limit(^@page_size)
      |> Repo.all()

    %{
      entries: entries,
      page_number: page_num,
      total_pages: total_pages,
      total_entries: total
    }
  end

  defp parse_value_input(input) when is_binary(input) do
    case Jason.decode(input) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, _} -> {:error, "Value must be a JSON object (e.g. {\"key\": \"value\"})"}
      {:error, _} -> {:error, "Invalid JSON: please enter a valid JSON object"}
    end
  end

  defp parse_value_input(input) when is_map(input), do: {:ok, input}
  defp parse_value_input(_), do: {:error, "Value is required"}

  def format_value(value) when is_map(value), do: Jason.encode!(value, pretty: true)
  def format_value(value), do: to_string(value)

  defp non_blank?(nil), do: false
  defp non_blank?(""), do: false
  defp non_blank?(_), do: true

  defp maybe_put(kw, _key, nil), do: kw
  defp maybe_put(kw, _key, ""), do: kw
  defp maybe_put(kw, key, value), do: Keyword.put(kw, key, value)
end
