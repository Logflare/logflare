defmodule LogflareWeb.Auth.OauthController do
  use LogflareWeb, :controller

  plug :fix_port_callback_url
  plug Ueberauth

  alias Logflare.Alerting
  alias Logflare.JSON
  alias Logflare.Repo
  alias Logflare.Sources.Source
  alias LogflareWeb.AuthController

  require Logger

  # configure callback port based on PHX_URL_* env vars
  # Ueberauth does not respect phoenix url configurations
  # https://github.com/ueberauth/ueberauth/blob/f5118071e2f1343e383ea97d89c69ff62b6a8629/lib/ueberauth/strategies/helpers.ex#L71
  # Furthermore, we cannot set the option at runtime
  defp fix_port_callback_url(%Plug.Conn{request_path: "/auth/" <> _} = conn, _opts) do
    port =
      Application.get_env(:logflare, LogflareWeb.Endpoint)
      |> Keyword.get(:url, [])
      |> Keyword.get(:port, conn.port)
      |> case do
        port when is_binary(port) -> String.to_integer(port)
        port -> port
      end

    %{conn | port: port}
  end

  # don't adjust other paths
  defp fix_port_callback_url(conn, _opts), do: conn

  def request(conn, params) do
    Logger.warning("Received unrecognized Oauth provider request", error_string: inspect(params))
    auth_error_redirect(conn)
  end

  def callback(
        %{assigns: %{ueberauth_auth: _auth}} = conn,
        %{"state" => "", "provider" => "slack"} = params
      ) do
    callback(conn, Map.drop(params, ["state"]))
  end

  def callback(
        %{assigns: %{ueberauth_auth: auth}} = conn,
        %{"state" => state, "provider" => "slack"} = _params
      )
      when is_binary(state) do
    state = JSON.decode!(state)

    case state do
      %{"action" => "save_hook_url", "source" => source} ->
        slack_hook_url = auth.extra.raw_info.token.other_params["incoming_webhook"]["url"]
        source_changes = %{slack_hook_url: slack_hook_url}

        changeset =
          Source.changeset(
            %Source{id: source["id"], name: source["name"], token: source["token"]},
            source_changes
          )

        case Repo.update(changeset) do
          {:ok, _source} ->
            conn
            |> put_flash(:info, "Slack connected!")
            |> redirect(to: Routes.source_path(conn, :edit, source["id"]))

          {:error, _changeset} ->
            conn
            |> put_flash(:error, "Something went wrong!")
            |> redirect(to: Routes.source_path(conn, :edit, source["id"]))
        end

      %{"action" => "save_hook_url", "alert_query_id" => id} ->
        url = auth.extra.raw_info.token.other_params["incoming_webhook"]["url"]
        alert_query = Alerting.get_alert_query!(id)

        case Alerting.update_alert_query(alert_query, %{slack_hook_url: url}) do
          {:ok, _alert_query} ->
            conn
            |> put_flash(:info, "Alert connected to Slack!")
            |> redirect(to: ~p"/alerts/#{id}")

          {:error, _changeset} = err ->
            Logger.error("Error when saving slack hook url for AleryQuery",
              error_string: inspect(err)
            )

            conn
            |> put_flash(:error, "Something went wrong!")
            |> redirect(to: ~p"/alerts/#{id}")
        end
    end
  end

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, %{"provider" => "google"} = _params) do
    auth_params = %{
      token: auth.credentials.token,
      email: auth.info.email,
      email_preferred: auth.info.email,
      provider: "google",
      image: auth.info.image,
      name: auth.info.name,
      provider_uid: generate_provider_uid(auth, auth.provider),
      valid_google_account: true
    }

    AuthController.check_invite_token_and_signin(conn, auth_params)
  end

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, _params) do
    auth_params = %{
      token: auth.credentials.token,
      email: auth.info.email,
      email_preferred: auth.info.email,
      provider: Atom.to_string(auth.provider),
      image: auth.info.image,
      name: auth.info.name,
      provider_uid: generate_provider_uid(auth, auth.provider)
    }

    AuthController.check_invite_token_and_signin(conn, auth_params)
  end

  def callback(
        %{assigns: %{ueberauth_failure: failure}} = conn,
        %{"provider" => provider} = params
      ) do
    Logger.warning("Oauth failure for #{provider}. #{inspect(failure)}",
      error_string: inspect(params)
    )

    auth_error_redirect(conn)
  end

  def callback(conn, params) do
    Logger.warning("Received unrecognized Oauth provider callback request",
      error_string: inspect(params)
    )

    auth_error_redirect(conn)
  end

  defp auth_error_redirect(conn) do
    conn
    |> put_flash(:error, "Authentication error! Please contact support if this continues.")
    |> redirect(to: ~p"/dashboard")
  end

  defp generate_provider_uid(auth, :slack) do
    auth.credentials.other.user_id
  end

  defp generate_provider_uid(auth, provider) when provider in [:google, :github] do
    if is_integer(auth.uid) do
      Integer.to_string(auth.uid)
    else
      auth.uid
    end
  end
end
