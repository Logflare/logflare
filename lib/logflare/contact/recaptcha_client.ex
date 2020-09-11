defmodule Logflare.Contact.RecaptchaClient do
  require Logger

  use Tesla

  plug Tesla.Middleware.BaseUrl, "https://www.google.com/recaptcha/api"

  plug Tesla.Middleware.Headers, [
    {"Content-type", "application/x-www-form-urlencoded"},
    {"Accept", "application/json"}
  ]

  plug Tesla.Middleware.DecodeJson

  plug Tesla.Middleware.FormUrlencoded

  adapter(Tesla.Adapter.Mint, timeout: 60_000, mode: :passive)

  @recaptcha_secret Application.get_env(:logflare, :recaptcha_secret)

  def verify(token) do
    body = %{secret: @recaptcha_secret, response: token}

    post("/siteverify", body)
  end
end
