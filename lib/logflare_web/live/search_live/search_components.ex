defmodule LogflareWeb.SearchLive.SearchComponents do
  use LogflareWeb, :html
  use Phoenix.Component

  alias LogflareWeb.Utils

  attr :user, Logflare.User, required: true
  attr :disabled, :boolean, default: false

  def create_menu(assigns) do
    ~H"""
    <.button_dropdown id="create-menu" disabled={@disabled}>
      <i class="fas fa-plus"></i>
      Create new...
      <:menu_item :if={Utils.flag("endpointsOpenBeta", @user)} heading="Endpoint">
        <.menu_link resource="endpoint" kind="events" />
      </:menu_item>
      <:menu_item :if={Utils.flag("endpointsOpenBeta", @user)}>
        <.menu_link resource="endpoint" kind="aggregates" />
      </:menu_item>
      <:menu_item :if={Utils.flag("alerts", @user)} heading="Alert">
        <.menu_link resource="alert" kind="events" />
      </:menu_item>
      <:menu_item :if={Utils.flag("alerts", @user)}>
        <.menu_link resource="alert" kind="aggregates" />
      </:menu_item>
      <:menu_item heading="Query">
        <.menu_link resource="query" kind="events" />
      </:menu_item>
      <:menu_item>
        <.menu_link resource="query" kind="aggregates" />
      </:menu_item>
    </.button_dropdown>
    """
  end

  attr :resource, :string, values: ["endpoint", "alert", "query"]
  attr :kind, :string, values: ["events", "aggregates"]

  defp menu_link(assigns) do
    assigns =
      assigns
      |> assign_new(:label, fn
        %{kind: "events"} -> "From search"
        %{kind: "aggregates"} -> "From chart"
      end)

    ~H"""
    <a phx-click="create_new" phx-value-resource={@resource} phx-value-kind={@kind} class="tw-block tw-text-gray-500 tw-no-underline" href="#">{@label}</a>
    """
  end
end
