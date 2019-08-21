defmodule LogflareWeb.ContactController do
  use LogflareWeb, :controller

  alias Ecto.Changeset
  alias Logflare.{Contact.Emails, Mailer, Contact}

  def contact(conn, _params) do
    changeset = Contact.changeset(%Contact{}, %{})

    render(conn, "contact.html", changeset: changeset)
  end

  def new(conn, %{"contact" => contact} = params) do
    changeset = Contact.changeset(%Contact{}, contact)

    case Changeset.apply_action(changeset, :insert) do
      {:ok, _changeset} ->
        Emails.contact(contact)
        |> Mailer.deliver()

        changeset = Contact.changeset(%Contact{}, %{})

        conn
        |> put_flash(:info, "Thanks! We'll be in touch!")
        |> render("contact.html", changeset: changeset)

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong. Check your submission!")
        |> render("contact.html", changeset: changeset)
    end
  end
end
