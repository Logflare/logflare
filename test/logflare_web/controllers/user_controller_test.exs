defmodule LogflareWeb.UserControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase

  alias Logflare.Users
  alias Logflare.Logs.RejectedEvents
  import Logflare.DummyFactory

  setup do
    u1 = insert(:user)
    u2 = insert(:user)
    {:ok, users: [u1, u2], conn: Phoenix.ConnTest.build_conn()}
  end

  describe "UserController" do
    test "users can't update restricted fields", %{
      conn: conn,
      users: [u1 | _]
    } do
      nope_token = Faker.String.base64()
      nope_api_quota = 1337
      nope_user_id = 1

      conn =
        conn
        |> assign(:user, u1)
        |> put("/account/edit", %{
          "user" => %{
            "name" => u1.name,
            "token" => nope_token,
            "api_quota" => nope_api_quota,
            "id" => nope_user_id
          }
        })

      s1_new = Users.get_by_id(u1.id)

      refute conn.assigns[:changeset]
      refute s1_new.token == nope_token
      refute s1_new.api_quota == nope_api_quota
      refute s1_new.id == nope_user_id
      assert html_response(conn, 401) =~ "Not allowed"
    end

    test "of allowed fields succeeds", %{
      conn: conn,
      users: [u1 | _]
    } do
      new = %{
        email: Faker.Internet.free_email(),
        provider: Faker.String.base64(),
        email_preferred: Faker.Internet.free_email(),
        name: Faker.String.base64(),
        image: Faker.Internet.image_url(),
        email_me_product: true,
        phone: Faker.Phone.EnUs.phone()
        # bigquery_project_id: Faker.String.base64()
      }

      conn =
        conn
        |> assign(:user, u1)
        |> put("/account/edit", %{"user" => new})

      s1_new =
        Users.get_by_id(u1.id)
        |> Map.from_struct()
        |> Map.take(Map.keys(new))

      refute conn.assigns[:changeset]
      assert s1_new == new
      assert html_response(conn, 302) =~ user_path(conn, :edit)
    end
  end
end
