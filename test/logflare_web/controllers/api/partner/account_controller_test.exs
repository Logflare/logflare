defmodule LogflareWeb.Api.Partner.AccountControllerTest do
  use LogflareWeb.ConnCase
  alias Logflare.Partners
  alias Logflare.Auth
  alias Logflare.Users

  setup do
    {:ok, %{partner: insert(:partner)}}
  end

  @allowed_fields MapSet.new(~w(api_quota company email name phone token))

  describe "index/2" do
    test "returns 200 and a list of users created by given partner", %{
      conn: conn,
      partner: partner
    } do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert [user_response] =
               conn
               |> add_access_token(partner, ~w(partner))
               |> get("/api/partner/accounts")
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
             |> get("/api/partner/accounts", %{email: email})
             |> json_response(401) == %{"error" => "Unauthorized"}
    end
  end

  describe "create/2" do
    test "returns 201 and the user information and a token to access the API", %{
      conn: conn,
      partner: partner
    } do
      email = TestUtils.gen_email()

      assert response =
               conn
               |> add_access_token(partner, ~w(partner))
               |> post("/api/partner/accounts", %{email: email})
               |> json_response(201)

      assert response["user"]["email"] == String.downcase(email)

      [%{token: token}] =
        email
        |> String.downcase()
        |> then(&Users.get_by(email: &1))
        |> Auth.list_valid_access_tokens()

      assert response["api_key"] == token
    end

    test "returns 400 when no email is given", %{conn: conn, partner: partner} do
      assert conn
             |> add_access_token(partner, ~w(partner))
             |> post("/api/partner/accounts")
             |> json_response(422)
    end

    test "return 401 with the wrong authentication token", %{conn: conn} do
      email = TestUtils.gen_email()

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> post("/api/partner/accounts", %{email: email})
             |> json_response(401) == %{"error" => "Unauthorized"}
    end
  end

  describe "get_account/2" do
    test "returns 200 and the user information", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert response =
               conn
               |> add_access_token(partner, ~w(partner))
               |> get("/api/partner/accounts/#{user.token}")
               |> json_response(200)

      assert response["token"] == user.token

      assert response
             |> Map.keys()
             |> MapSet.new()
             |> MapSet.equal?(@allowed_fields)
    end

    test "return 401 with the wrong auth token", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> get("/api/partner/accounts/#{user.token}")
             |> json_response(401) == %{"error" => "Unauthorized"}
    end

    test "return 404 when accessing a user from another partner", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(insert(:partner), %{"email" => TestUtils.gen_email()})

      assert conn
             |> add_access_token(partner, ~w(partner))
             |> get("/api/partner/accounts/#{user.token}")
             |> response(404)
    end
  end

  describe "get_account_usage/2" do
    test "returns 200 and the usage for a given user", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})
      %{count: count} = insert(:billing_counts, user: user)

      assert conn
             |> add_access_token(partner, ~w(partner))
             |> get("/api/partner/accounts/#{user.token}/usage")
             |> json_response(200) == %{"usage" => count}
    end

    test "return 401 with the wrong auth token", %{conn: conn, partner: partner} do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(partner, params)

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> get("/api/partner/accounts/#{user.token}/usage")
             |> json_response(401) == %{"error" => "Unauthorized"}
    end

    test "return 404 when accessing a user from another partner", %{conn: conn, partner: partner} do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(insert(:partner), params)

      assert conn
             |> add_access_token(partner, ~w(partner))
             |> get("/api/partner/accounts/#{user.token}/usage")
             |> response(404)
    end
  end

  describe "delete_user/2" do
    test "returns 204 and deletes the user", %{conn: conn, partner: partner} do
      {:ok, user} = Partners.create_user(partner, %{"email" => TestUtils.gen_email()})

      assert response =
               conn
               |> add_access_token(partner, ~w(partner))
               |> delete("/api/partner/accounts/#{user.token}")
               |> json_response(204)

      assert response["token"] == user.token

      assert response
             |> Map.keys()
             |> MapSet.new()
             |> MapSet.equal?(@allowed_fields)

      assert Partners.get_user_by_token(partner, user.token) == nil
    end

    test "returns 401 with the wrong auth token", %{conn: conn, partner: partner} do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(partner, params)

      assert conn
             |> put_req_header("authorization", "Bearer potato")
             |> delete("/api/partner/accounts/#{user.token}")
             |> json_response(401) == %{"error" => "Unauthorized"}
    end

    test "return 404 when accessing a user from another partner", %{conn: conn, partner: partner} do
      params = %{"email" => TestUtils.gen_email()}
      {:ok, user} = Partners.create_user(insert(:partner), params)

      assert conn
             |> add_access_token(partner, ~w(partner))
             |> get("/api/partner/accounts/#{user.token}")
             |> response(404)
    end
  end
end
