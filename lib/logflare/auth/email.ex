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
      "Sign in to Logflare with this link:\n\n#{
        Routes.email_url(Endpoint, :callback, Auth.gen_token(email))
      }\n\nOr copy and paste this token into the verification form:\n\n#{Auth.gen_token(email)}\n\nThis link and token are valid for 30 minutes."
    )
  end

  def auth_email_no_link(email) do
    new()
    |> to(email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Sign In to Logflare")
    |> text_body(
      "Copy and paste this token into the verification form:\n\n#{Auth.gen_token(email)}\n\nThis link and token are valid for 30 minutes."
    )
  end
end
