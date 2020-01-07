defmodule Logflare.Auth do
  @max_age_default 86_400

  alias Phoenix.Token

  @salt Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]

  def gen_token(email) when is_binary(email) do
    Token.sign(LogflareWeb.Endpoint, @salt, email)
  end

  def gen_token(id) when is_integer(id) do
    Token.sign(LogflareWeb.Endpoint, @salt, id)
  end

  def gen_token(map) when is_map(map) do
    Token.sign(LogflareWeb.Endpoint, @salt, map)
  end

  def verify_token(token, max_age \\ @max_age_default) do
    Token.verify(LogflareWeb.Endpoint, @salt, token, max_age: max_age)
  end
end
