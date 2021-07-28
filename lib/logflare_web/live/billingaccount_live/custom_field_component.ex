defmodule LogflareWeb.BillingAccountLive.CustomFieldComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML
  import LogflareWeb.ErrorHelpers

  alias Logflare.BillingAccounts

  require Logger

  def preload(assigns) when is_list(assigns) do
    assigns
  end

  def mount(socket) do
    {:ok, socket}
  end

  def update(assigns, socket) do
    ba = assigns.billing_account

    socket =
      socket
      |> assign(billing_account: ba)
      |> assign(changeset: BillingAccounts.change_billing_account(ba))

    {:ok, socket}
  end

  def handle_event("validate", %{"fields" => _params}, socket) do
    :noop

    {:noreply, socket}
  end

  def handle_event("delete", %{"key" => key}, socket) do
    ba = socket.assigns.billing_account

    fields = Enum.filter(ba.custom_invoice_fields, fn %{"name" => x} -> x != key end)

    with {:ok, ba} <-
           BillingAccounts.update_billing_account(ba, %{custom_invoice_fields: fields}),
         {:ok, _customer} <-
           BillingAccounts.Stripe.update_customer(ba.stripe_customer, %{
             invoice_settings: %{custom_fields: fields}
           }) do
      socket =
        socket
        |> assign(billing_account: ba)
        |> clear_flash()
        |> put_flash(:info, "Custom field deleted!")
        |> push_patch(to: Routes.billing_account_path(socket, :edit))

      {:noreply, socket}
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply, assign(socket, changeset: changeset)}

      _err ->
        error_socket(socket)
    end
  end

  def handle_event("add", %{"fields" => params}, socket) do
    ba = socket.assigns.billing_account
    f = if ba.custom_invoice_fields, do: ba.custom_invoice_fields, else: []
    fields = f ++ [params]

    with {:ok, _customer} <-
           BillingAccounts.Stripe.update_customer(ba.stripe_customer, %{
             invoice_settings: %{custom_fields: fields}
           }),
         {:ok, ba} <-
           BillingAccounts.update_billing_account(ba, %{custom_invoice_fields: fields}) do
      socket =
        socket
        |> assign(billing_account: ba)
        |> clear_flash()
        |> put_flash(:info, "Custom field added!")
        |> push_patch(to: Routes.billing_account_path(socket, :edit))

      {:noreply, socket}
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply, assign(socket, changeset: changeset)}

      {:error,
       %Stripe.Error{
         message: "Array invoice_settings[custom_fields] exceeded maximum 4 allowed elements."
       }} ->
        socket =
          socket
          |> put_flash(:error, "Maximum 4 custom invoice fields!")
          |> push_patch(to: Routes.billing_account_path(socket, :edit))

        {:noreply, socket}

      _err ->
        error_socket(socket)
    end
  end

  def render(assigns) do
    ~L"""
    <%= if @billing_account.custom_invoice_fields do %>
    <ul class="list-unstyled">
    <%=  for %{"name" => k, "value" => v} <- @billing_account.custom_invoice_fields do %>
      <li><%= k %>: <%= v %> <%= delete_link(k, @myself) %></li>
    <% end %>
    </ul>
    <% end %>
    <%= f = form_for :fields, "#", [phx_change: :validate, phx_submit: :add, phx_target: @myself] %>
    <div class="row">
    <div class="col-sm">
    <%= label f, :name, class: "label-padding" %>
    <%= text_input f, :name, class: "form-control form-control-margin" %>
    <%= error_tag f, :name %>
    </div>
    <div class="col-md">
    <%= label f, :value, class: "label-padding" %>
    <%= text_input f, :value, class: "form-control form-control-margin" %>
    <%= error_tag f, :value %>
    </div>
    <div class="col-md">
    </div>
    </div>
    <%= submit "Add", phx_disable_with: "Adding...", class: "btn btn-primary form-button mt-4" %>
    </form>
    """
  end

  defp delete_link(key, myself) do
    ~E"""
    <a href="#" phx-click="delete" phx-disable-with="deleting..." phx-value-key="<%= key %>" phx-target="<%= myself %>" class="small">delete</a>
    """
  end

  defp error_socket(socket) do
    socket =
      socket
      |> clear_flash()
      |> put_flash(:info, "Something went wrong! Please contact support if this continues.")
      |> push_patch(to: Routes.billing_account_path(socket, :edit))

    {:noreply, socket}
  end
end
