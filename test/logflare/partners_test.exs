defmodule Logflare.PartnerTest do
  use Logflare.DataCase

  alias Logflare.Partners
  alias Logflare.Partners.PartnerUser
  alias Logflare.Repo
  alias Logflare.User

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

      partner = Repo.preload(partner, :users)
      assert [_user] = partner.users
      assert user.email == String.downcase(email)
    end
  end

  describe "get_partner_by_token/1" do
    test "nil if not found" do
      assert is_nil(Partners.get_partner_by_token(TestUtils.gen_uuid()))
    end

    test "partner struct if exists" do
      %{token: token} = partner = insert(:partner)
      assert partner == Partners.get_partner_by_token(token)
    end
  end

  describe "delete_partner_by_token/1" do
    test "deletes partner using token" do
      %{token: token} = insert(:partner)
      assert {:ok, _} = Partners.delete_partner_by_token(token)
    end
  end

  describe "get_user_by_token/2" do
    test "fetches user if user was created by given partner" do
      partner = insert(:partner)
      email = TestUtils.gen_email()
      {:ok, %{token: token}} = Partners.create_user(partner, %{"email" => email})

      result = Partners.get_user_by_token(partner, token)
      assert token == result.token
    end

    test "nil if user was not created by given partner" do
      partner = insert(:partner)
      email = TestUtils.gen_email()

      {:ok, %{token: token}} = Partners.create_user(insert(:partner), %{"email" => email})

      assert is_nil(Partners.get_user_by_token(partner, token))
    end
  end

  describe "delete_user/2" do
    test "deletes user and removes association with partner" do
      stub(Goth, :fetch, fn _ -> {:error, ""} end)

      partner = insert(:partner)

      {:ok, %{id: id} = user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      {:ok, %{id: not_deleted_id}} =
        Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert {:ok, %User{id: ^id}} = Partners.delete_user(partner, user)

      partner = Repo.preload(partner, :users)
      assert [%User{id: ^not_deleted_id}] = partner.users
    end

    test "does not delete user if user was created by another partner" do
      partner = insert(:partner)
      {:ok, user} = Partners.create_user(insert(:partner), %{"email" => TestUtils.gen_email()})
      assert {:error, :not_found} = Partners.delete_user(partner, user)
    end
  end

  test "upgrade_user/2, downgrade_user/2, user_upgraded?/1" do
    user = insert(:user)
    partner = insert(:partner, users: [user])
    assert Partners.user_upgraded?(user) == false
    assert {:ok, %PartnerUser{upgraded: true}} = Partners.upgrade_user(partner, user)
    assert Partners.user_upgraded?(user)
    assert {:ok, %PartnerUser{upgraded: false}} = Partners.downgrade_user(partner, user)
    assert Partners.user_upgraded?(user) == false

    assert Partners.user_upgraded?(insert(:user)) == false

    assert {:error, :not_found} = Partners.upgrade_user(partner, insert(:user))
  end

end
