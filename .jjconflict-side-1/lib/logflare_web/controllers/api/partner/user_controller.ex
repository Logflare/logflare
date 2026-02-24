defmodule LogflareWeb.Api.Partner.UserController do
  use LogflareWeb, :controller

  alias Logflare.Partners
  alias Logflare.Users
  alias Logflare.Billing.BillingCounts
  alias Logflare.Auth
  alias Logflare.Users

  action_fallback(LogflareWeb.Api.FallbackController)
  @allowed_fields [:email, :name, :partner_upgraded, :token, :metadata]
  def index(%{assigns: %{partner: partner}} = conn, params) do
    metadata = Map.get(params, "metadata")

    with users <- Users.list_users(partner_id: partner.id, metadata: metadata) do
      allowed_response = Enum.map(users, &sanitize_response/1)
      json(conn, allowed_response)
    end
  end

  def create(%{assigns: %{partner: partner}} = conn, params) do
    with {:ok, user} <- Partners.create_user(partner, params) do
      {:ok, %{token: token}} =
        Auth.create_access_token(user, %{description: "Default auto-generated"})

      conn
      |> put_status(201)
      |> json(%{user: user, api_key: token})
    end
  end

  def get_user(%{assigns: %{partner: partner}} = conn, %{"user_token" => user_token}) do
    with {:ok, user} <- Partners.fetch_user_by_uuid(partner, user_token) do
      allowed_response = sanitize_response(user)
      json(conn, allowed_response)
    end
  end

  def get_user_usage(%{assigns: %{partner: partner}} = conn, %{"user_token" => user_token}) do
    with {:ok, user} <- Partners.fetch_user_by_uuid(partner, user_token) do
      end_date = DateTime.utc_now()

      start_date =
        end_date
        |> then(&Date.new!(&1.year, &1.month, &1.day))
        |> Date.beginning_of_month()
        |> DateTime.new!(~T[00:00:00])

      usage = BillingCounts.cumulative_usage(user, start_date, end_date)
      json(conn, %{usage: usage})
    end
  end

  def delete_user(%{assigns: %{partner: partner}} = conn, %{"user_token" => user_token}) do
    with {:ok, user} <- Partners.fetch_user_by_uuid(partner, user_token),
         {:ok, user} <- Partners.delete_user(partner, user) do
      allowed_response = sanitize_response(user)

      conn
      |> put_status(204)
      |> json(allowed_response)
    end
  end

  def upgrade(%{assigns: %{partner: partner}} = conn, %{"user_token" => user_token}) do
    with {:ok, user} <- Partners.fetch_user_by_uuid(partner, user_token),
         {:ok, %_{} = user} <- Partners.upgrade_user(user) do
      json(conn, Map.take(user, @allowed_fields))
    end
  end

  def downgrade(%{assigns: %{partner: partner}} = conn, %{"user_token" => user_token}) do
    with {:ok, user} <- Partners.fetch_user_by_uuid(partner, user_token),
         {:ok, %_{} = user} <- Partners.downgrade_user(user) do
      json(conn, Map.take(user, @allowed_fields))
    end
  end

  defp sanitize_response(user) do
    Enum.reduce(@allowed_fields, %{}, fn key, acc -> Map.put(acc, key, Map.get(user, key)) end)
  end
end
