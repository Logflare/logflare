defmodule Logflare.Admin.Emails do
  @moduledoc false
  import Swoosh.Email

  @doc """
  Email message for contacting support@logflare.app
  """
  @typep form_data :: %{String.t => String.t }
  @spec contact_email(form_data()) :: Swoosh.Email.t()
  def contact_email(
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
