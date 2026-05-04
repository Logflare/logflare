defmodule LogflareWeb.UserSocket do
  use Phoenix.Socket, log: false

  alias Logflare.Auth
  alias Logflare.Repo
  alias Logflare.User

  channel "source:*", LogflareWeb.SourceChannel

  def connect(%{"token" => _token, "public_token" => public_token}, socket)
      when public_token != "undefined" do
    case Auth.verify_public_source_token(public_token) do
      {:ok, public_token} -> {:ok, assign(socket, :public_token, public_token)}
      {:error, _reason} -> :error
    end
  end

  def connect(%{"token" => token, "public_token" => _public_token}, socket)
      when token != "undefined" do
    with {:ok, user_id} <- Auth.verify_user_socket_token(token),
         user <- Repo.get(User, user_id) |> Repo.preload(:sources) do
      {:ok, assign(socket, :user, user)}
    else
      {:error, _reason} ->
        :error
    end
  end

  def connect(_params, _socket) do
    :error
  end

  def id(socket) do
    case Map.get(socket, :user) do
      %User{} = user ->
        "user_socket:user:#{user.id}"

      nil ->
        anon_id = Ecto.UUID.generate()
        "user_socket:anon:#{anon_id}"
    end
  end
end
