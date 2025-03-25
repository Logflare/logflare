defmodule LogflareWeb.AlertsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  require Logger

  alias Logflare.Users
  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery

  embed_templates("actions/*", suffix: "_action")
  embed_templates("components/*")

  def render(%{live_action: :index} = assigns), do: index_action(assigns)
  def render(%{live_action: :show, alert: nil} = assigns), do: not_found_action(assigns)
  def render(%{live_action: :show} = assigns), do: show_action(assigns)
  def render(%{live_action: :new} = assigns), do: new_action(assigns)
  def render(%{live_action: :edit} = assigns), do: edit_action(assigns)

  defp render_docs_link(assigns) do
    ~H"""
    <.subheader_link to="https://docs.logflare.app/concepts/endpoints" text="docs" fa_icon="book" />
    """
  end

  defp render_access_tokens_link(assigns) do
    ~H"""
    <.subheader_link to={~p"/access-tokens"} text="access tokens" fa_icon="key" />
    """
  end

  def mount(%{}, %{"user_id" => user_id}, socket) do
    user = Users.get(user_id)

    socket =
      socket
      |> assign(:user_id, user_id)
      |> assign(:user, user)
      #  must be below user_id assign
      |> refresh()
      |> assign(:query_result_rows, nil)
      |> assign(:alert, nil)
      |> assign(:endpoint_changeset, Alerting.change_alert_query(%AlertQuery{}))
      |> assign(:base_url, LogflareWeb.Endpoint.url())
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:params_form, to_form(%{"query" => "", "params" => %{}}, as: "run"))
      |> assign(:declared_params, %{})

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    alert_id = params["id"]

    alert =
      if alert_id do
        Alerting.get_alert_query_by(id: alert_id, user_id: socket.assigns.user_id)
        |> Alerting.preload_alert_query()
      end

    socket = assign(socket, :alert, alert)

    socket =
      if socket.assigns.live_action == :edit do
        assign(socket, :changeset, Alerting.change_alert_query(alert))
      else
        assign(socket, :changeset, nil)
      end

    {:noreply, socket}
  end

  def handle_event(
        "save",
        %{"alert" => params},
        %{assigns: %{user: user, alert: alert}} = socket
      ) do
    Logger.debug("Saving alert", params: params)

    case upsert_alert(alert, user, params) do
      {:ok, updated_alert} ->
        verb = if alert, do: "updated", else: "created"

        {:noreply,
         socket
         |> assign(:alert, updated_alert)
         |> put_flash(:info, "Successfully #{verb} alert #{updated_alert.name}")
         |> push_patch(to: ~p"/alerts/#{updated_alert.id}")}

      {:error, %Ecto.Changeset{} = changeset} ->
        verb = if alert, do: "update", else: "create"

        message = "Could not #{verb} alert. Please fix the errors before trying again."

        socket =
          socket
          |> put_flash(:info, message)
          |> assign(:changeset, changeset)

        {:noreply, socket}
    end
  end

  def handle_event(
        "delete",
        %{"alert_id" => id},
        %{assigns: _assigns} = socket
      ) do
    alert = Alerting.get_alert_query!(id)
    {:ok, _} = Alerting.delete_alert_query(alert)

    {:noreply,
     socket
     |> refresh()
     |> assign(:alert, nil)
     |> put_flash(:info, "#{alert.name} has been deleted")
     |> push_patch(to: "/alerts")}
  end

  def handle_event(
        "remove-slack",
        _params,
        %{assigns: %{alert: %_{id: alert_id}}} = socket
      ) do
    alert = Alerting.get_alert_query!(alert_id)

    with {:ok, alert} <- Alerting.update_alert_query(alert, %{slack_hook_url: nil}) do
      {:noreply,
       socket
       |> assign(:alert, alert)
       |> put_flash(:info, "Slack notifications have been removed.")}
    end
  end

  def handle_event("clear-results", _params, socket) do
    {:noreply,
     socket
     |> assign(:query_result_rows, nil)
     |> put_flash(:info, "Query run results has been cleared")}
  end

  def handle_event(
        "run-query",
        _params,
        %{assigns: %{alert: %_{} = alert}} = socket
      ) do
    with {:ok, result} <- Alerting.execute_alert_query(alert) do
      {:noreply,
       socket
       |> assign(:query_result_rows, result)
       |> put_flash(:info, "Alert has been triggered. Notifications sent!")}
    else
      {:error, :no_results} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Alert has been triggered. No results from query, notifications not sent!"
         )}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Error when running query: #{inspect(err)}"
         )}
    end
  end

  def handle_event(
        "manual-trigger",
        _params,
        %{assigns: %{alert: %_{} = alert}} = socket
      ) do
    with :ok <- Alerting.run_alert(alert) do
      {:noreply,
       socket
       |> put_flash(:info, "Alert has been triggered. Notifications sent!")}
    else
      {:error, :no_results} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Alert has been triggered. No results from query, notifications not sent!"
         )}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Error when running query: #{inspect(err)}"
         )}
    end
  end

  defp refresh(%{assigns: assigns} = socket) do
    alerts = Alerting.list_alert_queries(assigns.user)

    assign(socket, :alerts, alerts)
  end

  defp upsert_alert(alert, user, params) do
    with {:ok, alert} <-
           (case alert do
              nil -> Alerting.create_alert_query(user, params)
              %_{} -> Alerting.update_alert_query(alert, params)
            end),
         {:ok, _citrine_job} <- Alerting.upsert_alert_job(alert) do
      {:ok, alert}
    end
  end
end
