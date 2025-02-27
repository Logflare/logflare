defmodule Logflare.PartnerTest do
  use Logflare.DataCase

  alias Logflare.Partners
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Google.CloudResourceManager
  import Ecto.Query

  describe "get/1" do
    test "returns the partner with given id" do
      partner = insert(:partner)
      assert partner == Partners.get_partner(partner.id)
    end
  end

  describe "new_partner/2" do
    test "inserts a new partner" do
      {:ok, partner} = Partners.create_partner(TestUtils.random_string())
      assert partner
      assert partner.token
    end
  end

  describe "list_partners/0" do
    test "lists all partners" do
      partner = insert(:partner)
      assert [partner] == Partners.list_partners()
    end
  end

  describe "create_user/2" do
    test "creates new user and associates with given partner" do
      partner = insert(:partner)
      email = TestUtils.gen_email()

      assert {:ok, user} = Partners.create_user(partner, %{"email" => email})
      assert user.partner_id == partner.id
      assert Repo.preload(user, :partner).partner.id == partner.id
    end
  end

  describe "get_partner_by_uuid/1" do
    test "nil if not found" do
      assert is_nil(Partners.get_partner_by_uuid(TestUtils.gen_uuid()))
    end

    test "partner struct if exists" do
      %{token: token} = partner = insert(:partner)
      assert partner == Partners.get_partner_by_uuid(token)
    end
  end

  describe "delete_partner_by_token/1" do
    test "deletes partner using token" do
      %{token: token} = insert(:partner)
      assert {:ok, _} = Partners.delete_partner_by_token(token)
    end
  end

  describe "get_user_by_uuid/2" do
    test "fetches user if user was created by given partner" do
      partner = insert(:partner)
      email = TestUtils.gen_email()
      {:ok, %{id: id} = user} = Partners.create_user(partner, %{"email" => email})
      assert %User{id: ^id} = Partners.get_user_by_uuid(partner, user.token)
    end

    test "nil if user was not created by given partner" do
      partner = insert(:partner)
      email = TestUtils.gen_email()

      {:ok, %{token: token}} = Partners.create_user(insert(:partner), %{"email" => email})

      assert is_nil(Partners.get_user_by_uuid(partner, token))
    end
  end

  describe "delete_user/2" do
    test "deletes user and removes association with partner" do
      CloudResourceManager
      |> expect(:set_iam_policy, fn -> nil end)

      partner = insert(:partner)

      {:ok, %{id: id} = user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert {:ok, %{id: _}} =
               Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert {:ok, %User{id: ^id}} = Partners.delete_user(partner, user)
      refute Partners.get_user_by_uuid(partner, user.token)
    end

    test "does not delete user if user was created by another partner" do
      partner = insert(:partner)
      {:ok, user} = Partners.create_user(insert(:partner), %{"email" => TestUtils.gen_email()})
      assert {:error, :not_found} = Partners.delete_user(partner, user)
    end
  end

  test "upgrade_user/2, downgrade_user/2, user_upgraded?/1" do
    partner = insert(:partner)
    user = insert(:user, partner: partner)
    assert Partners.user_upgraded?(user) == false
    assert {:ok, %User{partner_upgraded: true} = user} = Partners.upgrade_user(user)
    assert Partners.user_upgraded?(user)

    assert {:ok, %User{partner_upgraded: false} = user} = Partners.downgrade_user(user)

    assert Partners.user_upgraded?(user) == false

    assert Partners.user_upgraded?(insert(:user)) == false

    assert {:error, :no_partner} = Partners.upgrade_user(insert(:user))
  end

  test "backwards compat: user upgrading with partner_users table, do upgrading" do
    partner = insert(:partner)
    user = insert(:user, partner: partner)

    Repo.insert_all("partner_users", [
      %{partner_id: partner.id, user_id: user.id, upgraded: false}
    ])

    assert Partners.user_upgraded?(user) == false
    assert {:ok, %User{partner_upgraded: true} = user} = Partners.upgrade_user(user)
    assert Repo.one(from(pu in "partner_users", select: pu.upgraded)) == true
    assert Partners.user_upgraded?(user)
  end

  test "backwards compat: user downgrading with partner_users table, do downgrade" do
    partner = insert(:partner)
    user = insert(:user, partner: partner, partner_upgraded: true)

    Repo.insert_all("partner_users", [%{partner_id: partner.id, user_id: user.id, upgraded: true}])

    assert Partners.user_upgraded?(user)

    assert {:ok, %User{partner_upgraded: false} = user} = Partners.downgrade_user(user)

    refute Partners.user_upgraded?(user)
    refute Repo.one(from(pu in "partner_users", select: pu.upgraded))
  end
end
