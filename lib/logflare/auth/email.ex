defmodule Logflare.Auth.Email do
  import Swoosh.Email

  alias Logflare.Auth
  alias LogflareWeb.Endpoint
  alias LogflareWeb.Router.Helpers, as: Routes

  def auth_email(email) do
    new()
    |> to(email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Sign In to Logflare")
    |> text_body(
      "Sign into Logflare:\n\n#{Routes.email_url(Endpoint, :callback, Auth.gen_token(email))}\n\nThis link is valid for 30 minutes."
    )
  end
end
