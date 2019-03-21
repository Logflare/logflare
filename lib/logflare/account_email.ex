defmodule Logflare.AccountEmail do
  import Swoosh.Email

  def welcome(user) do
    new()
    |> to(user.email)
    |> from({"Chase Granberry", "chase.granberry@gmail.com"})
    |> subject("Welcome to Logflare!")
    |> text_body(
      "Yo!\n\nThanks for checking out Logflare! Let me know if you have any issues :)\n\nIf you don't like it you can always delete your account here: https://logflare.app/account/edit"
    )
  end
end
