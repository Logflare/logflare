defmodule Logflare.Partners.PartnerUser do
  @moduledoc """
  Handles PartnerUser
  """
  use Ecto.Schema
  import Ecto.Changeset

  schema "partner_users" do
    belongs_to :partner, Logflare.Partners.Partner
    belongs_to :user, Logflare.User
    field :upgraded, :boolean, default: false
  end

  def changeset(partner_user, params) do
    partner_user
    |> cast(params, [:partner_id, :user_id])
    |> validate_required([:partner_id, :user_id])
    |> unique_constraint([:partner_id, :user_id])
  end
end
