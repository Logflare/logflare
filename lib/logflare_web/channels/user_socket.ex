defmodule LogflareWeb.UserSocket do
  use Phoenix.Socket

  alias Logflare.User
  alias Logflare.Repo

  @salt Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]
  @max_age 86_400

  channel("source:*", LogflareWeb.SourceChannel)
  channel("dashboard:*", LogflareWeb.DashboardChannel)

  def connect(%{"token" => "undefined", "public_token" => "undefined"}, socket) do
    :error
  end

  def connect(%{"token" => token, "public_token" => public_token}, socket)
      when token == "undefined" do
    if {:ok, public_token} = verify_token(public_token) do
      {:ok, assign(socket, :public_token, public_token)}
    else
      :error
    end
  end

  def connect(%{"token" => token, "public_token" => public_token}, socket)
      when public_token == "undefined" do
    with {:ok, user_id} <- verify_token(token),
         user <- Repo.get(User, user_id) |> Repo.preload(:sources) do
      {:ok, assign(socket, :user, user)}
    else
      {:error, _reason} ->
        :error
    end
  end

  def id(_socket), do: nil

  def verify_token(token),
    do: Phoenix.Token.verify(LogflareWeb.Endpoint, @salt, token, max_age: @max_age)
end
