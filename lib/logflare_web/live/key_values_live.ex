defmodule LogflareWeb.KeyValuesLive do
  @moduledoc false
  use LogflareWeb, :live_view

  alias Logflare.Billing
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache

  @page_size 500

  def mount(_params, _session, socket) do
    %{assigns: %{user: user}} = socket

    socket =
      socket
      |> assign(:total_count, Cache.count(user.id))
      |> assign(:page, nil)
      |> assign(:searched?, false)
      |> assign(:filter_key, "")
      |> assign(:filter_value, "")
      |> assign(:show_create_form, false)

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    key = params["key"]
    value = params["value"]

    socket =
      if non_blank?(key) || non_blank?(value) do
        page = params["page"] || "1"

        opts =
          [user_id: socket.assigns.user.id, page: String.to_integer(page), page_size: @page_size]
          |> maybe_put(:key, key)
          |> maybe_put(:value, value)

        result = KeyValues.list_key_values_paginated(opts)

        socket
        |> assign(:page, result)
        |> assign(:searched?, true)
        |> assign(:filter_key, key || "")
        |> assign(:filter_value, value || "")
      else
        socket
        |> assign(:page, nil)
        |> assign(:searched?, false)
        |> assign(:filter_key, "")
        |> assign(:filter_value, "")
      end

    {:noreply, socket}
  end

  def handle_event("search", params, socket) do
    key = params["key"]
    value = params["value"]

    if non_blank?(key) || non_blank?(value) do
      query_params =
        %{}
        |> maybe_put_param("key", key)
        |> maybe_put_param("value", value)

      {:noreply, push_patch(socket, to: ~p"/key-values?#{query_params}")}
    else
      {:noreply, put_flash(socket, :error, "At least one filter is required")}
    end
  end

  def handle_event("create", params, socket) do
    user = socket.assigns.user
    plan = Billing.get_plan_by_user(user)
    current_count = KeyValues.count_key_values(user.id)

    if current_count >= plan.limit_key_values do
      {:noreply, put_flash(socket, :error, "Key-value limit of #{plan.limit_key_values} reached")}
    else
      case KeyValues.create_key_value(%{
             user_id: user.id,
             key: params["key"],
             value: params["value"]
           }) do
        {:ok, _kv} ->
          Cache.bust_by(user_id: user.id, key: params["key"])

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
  end

  def handle_event("delete", %{"id" => id}, socket) do
    user = socket.assigns.user
    kv = KeyValues.get_key_value(id)

    if kv && kv.user_id == user.id do
      {:ok, _} = KeyValues.delete_key_value(kv)
      Cache.bust_by(user_id: user.id, key: kv.key)

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

  defp refresh_search(socket) do
    if socket.assigns.searched? do
      opts =
        [user_id: socket.assigns.user.id, page: 1, page_size: @page_size]
        |> maybe_put(:key, socket.assigns.filter_key)
        |> maybe_put(:value, socket.assigns.filter_value)

      result = KeyValues.list_key_values_paginated(opts)
      assign(socket, :page, result)
    else
      socket
    end
  end

  defp non_blank?(nil), do: false
  defp non_blank?(""), do: false
  defp non_blank?(_), do: true

  defp maybe_put(kw, _key, nil), do: kw
  defp maybe_put(kw, _key, ""), do: kw
  defp maybe_put(kw, key, value), do: Keyword.put(kw, key, value)

  defp maybe_put_param(map, _key, nil), do: map
  defp maybe_put_param(map, _key, ""), do: map
  defp maybe_put_param(map, key, value), do: Map.put(map, key, value)
end
