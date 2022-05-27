defmodule Logflare.Admin.Contact do
  @moduledoc """
  Modue used for sending contact email to support. Ecto.Schema is used purely for type validation.

  TODO: replace with Params.
  """
  use TypedEctoSchema
  alias Logflare.Admin.Contact
  import Ecto.Changeset

  typed_schema "contact" do
    field :name, :string
    field :email, :string
    field :subject, :string
    field :body, :string
  end

  @spec changeset(%Contact{}, map()) :: %Ecto.Changeset{}
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
