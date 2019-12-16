defmodule Logflare.Auth do
  alias Phoenix.Token

  @salt Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]

  def gen_token(email) do
    Token.sign(LogflareWeb.Endpoint, @salt, email)
  end

  def verify_token(token, max_age) do
    Token.verify(LogflareWeb.Endpoint, @salt, token, max_age: max_age)
  end
end
