defmodule LogflareWeb.Api.Partner.UserControllerTest do
  use LogflareWeb.ConnCase
  alias Logflare.Partners

  setup do
    [partner: insert(:partner)]
  end

  @allowed_fields MapSet.new(~w(partner_upgraded email name token metadata))

  describe "index/2" do
    test "returns 200 and a list of users created by given partner", %{
      conn: conn,
      partner: partner
    } do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert [user_response] =
               conn
               |> add_access_token(partner, ~w(partner))
               |> get("/api/partner/users")
               |> json_response(200)

      assert user_response["token"] == user.token

      assert user_response
             |> Map.keys()
             |> MapSet.new()
             |> MapSet.equal?(@allowed_fields)
    end

    test "return 401 with the wrong authentication token", %{conn: conn} do
      email = TestUtils.gen_email()

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> get(~p"/api/partner/users", %{email: email})
             |> json_response(401) == %{"error" => "Unauthorized"}
    end
  end

  describe "POST user" do
    test "returns 201 and the user information and a token to access the API",
         %{
           conn: conn,
           partner: partner
         } do
      assert response =
               conn
               |> add_partner_access_token(partner)
               |> post(~p"/api/partner/users", %{
                 "email" => TestUtils.gen_email(),
                 "provider" => "email",
                 "metadata" => %{"my" => "value"}
               })
               |> json_response(201)

      assert response["user"]["metadata"]["my"] == "value"
      assert response["api_key"]
    end

    test "get by metadata search", %{conn: conn} do
      insert(:user,
        metadata: %{
          my_value: "other_value"
        }
      )

      partner = insert(:partner)

      insert(:user,
        partner: partner,
        metadata: %{
          my_value: "test"
        }
      )

      assert [user] =
               conn
               |> add_partner_access_token(partner)
               |> get(~p"/api/partner/users?#{%{metadata: %{my_value: "test"}}}")
               |> json_response(200)

      assert assert user["metadata"]["my_value"] == "test"
    end

    test "return 401 with the wrong authentication token", %{conn: conn} do
      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> post(~p"/api/partner/users", %{})
             |> json_response(401) == %{"error" => "Unauthorized"}
    end
  end

  describe "GET user" do
    test "returns 200 and the user information", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert response =
               conn
               |> add_partner_access_token(partner)
               |> get(~p"/api/partner/users/#{user.token}")
               |> json_response(200)

      assert response["token"] == user.token
      # no details set yet on user
      assert response["partner_details"] == nil

      assert response
             |> Map.keys()
             |> MapSet.new()
             |> MapSet.equal?(@allowed_fields)
    end

    test "return 401 with the wrong auth token", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> get(~p"/api/partner/users/#{user.token}")
             |> json_response(401) == %{"error" => "Unauthorized"}
    end

    test "return 404 when accessing a user from another partner", %{
      conn: conn,
      partner: partner
    } do
      {:ok, user} =
        Partners.create_user(insert(:partner), %{
          "email" => TestUtils.gen_email()
        })

      assert conn
             |> add_partner_access_token(partner)
             |> get(~p"/api/partner/users/#{user.token}")
             |> response(404)
    end
  end

  describe "PUT user tiers" do
    test "upgrade/downgrade", %{conn: conn} do
      partner = insert(:partner)
      user = insert(:user, partner: partner)

      # upgrade
      assert %{"partner_upgraded" => true} =
               conn
               |> add_partner_access_token(partner)
               |> put(~p"/api/partner/users/#{user.token}/upgrade")
               |> json_response(200)

      # downgrade
      assert %{"partner_upgraded" => false} =
               conn
               |> recycle()
               |> add_partner_access_token(partner)
               |> put(~p"/api/partner/users/#{user.token}/downgrade")
               |> json_response(200)
    end
  end

  describe "GET user usage" do
    test "returns 200 and the usage for a given user", %{
      conn: conn,
      partner: partner
    } do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      %{count: count} = insert(:billing_counts, user: user)

      assert conn
             |> add_partner_access_token(partner)
             |> get(~p"/api/partner/users/#{user.token}/usage")
             |> json_response(200) == %{"usage" => count}
    end

    test "return 401 with the wrong auth token", %{conn: conn, partner: partner} do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(partner, params)

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> get(~p"/api/partner/users/#{user.token}/usage")
             |> json_response(401) == %{"error" => "Unauthorized"}
    end

    test "return 404 when accessing a user from another partner", %{
      conn: conn,
      partner: partner
    } do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(insert(:partner), params)

      assert conn
             |> add_partner_access_token(partner)
             |> get(~p"/api/partner/users/#{user.token}/usage")
             |> response(404)
    end
  end

  describe "DELETE user" do
    test "returns 204 and deletes the user", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert response =
               conn
               |> add_access_token(partner, ~w(partner))
               |> delete("/api/partner/users/#{user.token}")
               |> json_response(204)

      assert response["token"] == user.token

      assert response
             |> Map.keys()
             |> MapSet.new()
             |> MapSet.equal?(@allowed_fields)

      assert Partners.get_user_by_uuid(partner, user.token) == nil
    end

    test "returns 401 with the wrong auth token", %{
      conn: conn,
      partner: partner
    } do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(partner, params)

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> delete("/api/partner/users/#{user.token}")
             |> json_response(401) == %{"error" => "Unauthorized"}
    end

    test "return 404 when accessing a user from another partner", %{
      conn: conn,
      partner: partner
    } do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(insert(:partner), params)

      assert conn
             |> add_access_token(partner, ~w(partner))
             |> get("/api/partner/users/#{user.token}")
             |> response(404)
    end
  end
end
