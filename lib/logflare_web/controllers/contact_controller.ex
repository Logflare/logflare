defmodule LogflareWeb.ContactController do
  use LogflareWeb, :controller

  alias Ecto.Changeset
  alias Logflare.{Contact.Emails, Mailer, Contact}

  @recaptcha_site_key Application.get_env(:logflare, :recaptcha_site_key)
  @recaptcha_secret Application.get_env(:logflare, :recaptcha_secret)
  @recaptcha_config %{
    recaptcha_site_key: @recaptcha_site_key,
    recaptcha_secret: @recaptcha_secret
  }

  def contact(conn, _params) do
    changeset = Contact.changeset(%Contact{}, %{})

    render(conn, "contact.html", changeset: changeset, config: @recaptcha_config)
  end

  def new(conn, %{"contact" => %{"recaptcha_token" => token} = contact}) do
    changeset = Contact.changeset(%Contact{}, contact)

    case Contact.RecaptchaClient.verify(token) do
      {:ok, %Tesla.Env{body: %{"success" => true}}} ->
        case Changeset.apply_action(changeset, :insert) do
          {:ok, _changeset} ->
            Emails.contact(contact)
            |> Mailer.deliver()

            changeset = Contact.changeset(%Contact{}, %{})

            conn
            |> put_flash(:info, "Thanks! We'll be in touch!")
            |> render("contact.html", changeset: changeset, config: @recaptcha_config)

          {:error, changeset} ->
            conn
            |> put_flash(:error, "Something went wrong. Check your submission!")
            |> render("contact.html", changeset: changeset, config: @recaptcha_config)
        end

      _error ->
        conn
        |> put_flash(:error, "Something went wrong. Check your submission!")
        |> render("contact.html", changeset: changeset, config: @recaptcha_config)
    end
  end
end
