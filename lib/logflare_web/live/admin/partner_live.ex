defmodule LogflareWeb.Admin.PartnerLive do
  @moduledoc false
  use LogflareWeb, :live_view

  alias Logflare.Partners
  alias Logflare.Partners.Partner
  alias Logflare.Auth
  require Logger

  def mount(_params, _session, socket) do
    partners = Partners.list_partners()

    socket =
      socket
      |> assign(:partners, partners)
      |> assign(:changeset, Partner.changeset(%Partner{}, %{}))
      |> assign(:created_token, nil)

    {:ok, socket}
  end

  def handle_event("save", %{"partner" => %{"name" => name}}, socket) do
    {:ok, partner} = Partners.create_partner(name)
    {:noreply, update(socket, :partners, fn partners -> partners ++ [partner] end)}
  end

  def handle_event("delete", %{"token" => token}, socket) do
    {:ok, _} = Partners.delete_partner_by_token(token)
    {:noreply, assign(socket, :partners, Partners.list_partners())}
  end

  def handle_event("create-token", %{"token" => token, "description" => description}, socket) do
    partner = Partners.get_partner_by_uuid(token)
    {:ok, token} = Auth.create_access_token(partner, %{description: description})

    Logger.debug("Creating access token for partner, partner_id=#{inspect(partner.id)}")

    socket =
      socket
      |> assign(:partners, Partners.list_partners())
      |> assign(:created_token, token)

    {:noreply, socket}
  end

  def handle_event("dismiss-created-token", _, socket) do
    {:noreply, assign(socket, :created_token, nil)}
  end
end
