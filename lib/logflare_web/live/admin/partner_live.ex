defmodule LogflareWeb.Admin.PartnerLive do
  @moduledoc false
  use LogflareWeb, :live_view

  alias Logflare.Partners
  alias Logflare.Partners.Partner
  require Logger

  def mount(_params, _session, socket) do
    partners = Partners.list_partners()

    socket =
      socket
      |> assign(:partners, partners)
      |> assign(:changeset, Partner.changeset(%Partner{}, %{}))

    {:ok, socket}
  end

  def handle_event("save", %{"partner" => %{"name" => name}}, socket) do
    {:ok, partner} = Partners.new_partner(name)
    {:noreply, update(socket, :partners, fn partners -> partners ++ [partner] end)}
  end

  def handle_event("delete", %{"token" => token}, socket) do
    {:ok, _} = Partners.delete_partner_by_token(token)
    {:noreply, assign(socket, :partners, Partners.list_partners())}
  end
end
