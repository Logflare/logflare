defmodule Logflare.Contact.Emails do
  import Swoosh.Email

  def contact(
        %{"name" => name, "email" => email, "subject" => subject, "body" => body} = _form_data
      ) do
    new()
    |> to("support@logflare.app")
    |> from({"Logflare Support", "support@logflare.app"})
    |> reply_to({name, email})
    |> subject(subject)
    |> text_body(body)
  end
end
