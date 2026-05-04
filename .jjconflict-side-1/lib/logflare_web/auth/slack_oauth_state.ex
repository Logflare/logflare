defmodule LogflareWeb.Auth.SlackOauthState do
  @moduledoc """
  Signs and verifies the Slack OAuth `state` parameter.

  Binds the state to the initiating user so an attacker cannot forge a
  callback URL that a victim's session will accept (RFC 6749 §10.12).
  """

  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User

  @max_age 600

  @spec sign(User.t() | TeamUser.t(), map()) :: String.t()
  def sign(current_user, payload) when is_map(payload) do
    Phoenix.Token.sign(LogflareWeb.Endpoint, env_salt(), {principal(current_user), payload})
  end

  @spec verify(User.t() | TeamUser.t() | nil, String.t() | nil) ::
          {:ok, map()} | {:error, :invalid | :expired | :mismatched_user | :no_user}
  def verify(nil, _token), do: {:error, :no_user}

  def verify(current_user, token) when is_binary(token) do
    expected = principal(current_user)

    case Phoenix.Token.verify(LogflareWeb.Endpoint, env_salt(), token, max_age: @max_age) do
      {:ok, {^expected, payload}} when is_map(payload) -> {:ok, payload}
      {:ok, _} -> {:error, :mismatched_user}
      {:error, :expired} -> {:error, :expired}
      {:error, _} -> {:error, :invalid}
    end
  end

  def verify(_, _), do: {:error, :invalid}

  defp principal(%User{id: id}), do: {:user, id}
  defp principal(%TeamUser{id: id}), do: {:team_user, id}

  defp env_salt, do: Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]
end
