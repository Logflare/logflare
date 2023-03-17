defmodule LogflareWeb.AlertsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  alias Logflare.Alerting
  alias Logflare.Users
  @impl true
  def render(assigns) do
    ~L"""
    <%= live_react_component("Comp.SubHeader", %{
      paths: [%{to: "/alerts", label: "alerts"}],
      actions: []
      }, [id: "subheader"])
    %>
    <div class="tw-flex tw-flex-row tw-py-10 tw-px-4 h-full">
    <section class="tw-w-full">
      <%= live_react_component("Interfaces.AlertsPage", %{
          alerts: @alert_queries,
          }, [id: "alerts"])
      %>
    </section>
    </div>
    """
  end

  @impl true
  def mount(_params, %{"user_id" => user_id}, socket) do
    user = Users.get(user_id)

    socket =
      socket
      |> assign(:user, user)
      |> assign(:page_title, "Alerts")
      |> refresh_alert_queries()

    {:ok, socket}
  end

  @impl true
  def handle_event("update-alert", %{"id" => id, "alert_query" => alert_query_params}, socket) do
    alert_query = Alerting.get_alert_query!(id)

    case Alerting.update_alert_query(alert_query, alert_query_params) do
      {:ok, _alert_query} ->
        {:noreply,
         socket
         |> refresh_alert_queries()
         |> put_flash(:info, "Alert query updated successfully")}

      {:error, %Ecto.Changeset{} = changeset} ->
        # TODO: notify
        {:noreply, assign(socket, :changeset, changeset)}
    end
  end

  @impl true
  def handle_event("create-alert", %{"alert_query" => alert_query_params}, socket) do
    case Alerting.create_alert_query(socket.assigns.user, alert_query_params) do
      {:ok, _alert_query} ->
        {:noreply,
         socket
         |> refresh_alert_queries()
         |> put_flash(:info, "Alert query created successfully")}

      {:error, %Ecto.Changeset{} = changeset} ->
        # TODO: notify
        {:noreply, assign(socket, :changeset, changeset)}
    end
  end

  @impl true
  def handle_event("delete-alert", %{"id" => id}, socket) do
    alert_query = Alerting.get_alert_query!(id)
    {:ok, _} = Alerting.delete_alert_query(alert_query)

    {:noreply, refresh_alert_queries(socket)}
  end

  defp refresh_alert_queries(socket) do
    socket
    |> assign(:alert_queries, Alerting.list_alert_queries(socket.assigns.user))
  end
end
