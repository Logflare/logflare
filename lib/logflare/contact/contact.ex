defmodule Logflare.Contact do
  use TypedEctoSchema

  import Ecto.Changeset

  typed_schema "contact" do
    field :name, :string
    field :email, :string
    field :subject, :string
    field :body, :string
  end

  def changeset(form_fields, attrs) do
    form_fields
    |> cast(attrs, [
      :name,
      :email,
      :subject,
      :body
    ])
    |> validate_required([:name, :email, :subject, :body])
  end
end
