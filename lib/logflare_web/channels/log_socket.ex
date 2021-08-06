defmodule LogflareWeb.LogSocket do
  use Phoenix.Socket

  alias Logflare.Users

  channel "logs:*", LogflareWeb.LogChannel
  channel "logs:erlang:*", LogflareWeb.LogChannel
  channel "logs:erlang:logger:*", LogflareWeb.LogChannel
  channel "logs:erlang:lager:*", LogflareWeb.LogChannel
  channel "logs:elixir:*", LogflareWeb.LogChannel
  channel "logs:elixir:logger:*", LogflareWeb.LogChannel

  def connect(%{"api_key" => api_key}, socket) do
    user = Users.get_by(api_key: api_key)

    if user do
      {:ok, assign(socket, :user, user)}
    else
      :error
    end
  end

  def connect(_params, socket) do
    :error
  end

  def id(socket), do: "log_socket:user:#{socket.assigns.user.id}"
end
