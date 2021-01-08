defmodule LogflareWeb.UserControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase
  @moduletag :unboxed
  @moduletag :this
  use Mimic

  alias Logflare.Google.BigQuery
  import Logflare.Factory

  setup do
    u1 = insert(:user, bigquery_dataset_id: "test_dataset_id_1")
    u2 = insert(:user, bigquery_dataset_id: "test_dataset_id_2")

    {:ok, users: [u1, u2], conn: Phoenix.ConnTest.build_conn()}
  end

  describe "UserController update" do
    test "of restricted fields fails", %{
      conn: conn,
      users: [u1 | _]
    } do
      nope_token = Faker.String.base64()
      nope_api_quota = 1337
      nope_user_id = 1

      user_params = %{
        "name" => u1.name,
        "token" => nope_token,
        "api_quota" => nope_api_quota,
        "id" => nope_user_id
      }

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> put(
          "/account/edit",
          %{
            "user" => user_params
          }
        )

      u1_new = Users.get_user_by(id: u1.id)

      refute conn.assigns[:changeset]
      refute u1_new.token == nope_token
      refute u1_new.api_quota == nope_api_quota
      refute u1_new.id == nope_user_id
      assert redirected_to(conn, 302) =~ user_path(conn, :edit)
    end

    test "of allowed fields succeeds", %{
      conn: conn,
      users: [u1 | _]
    } do
      user_params = params_for(:user)

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> put("/account/edit", %{"user" => user_params})

      u1_new =
        Users.get_user_by(id: u1.id)
        |> Map.from_struct()

      refute conn.assigns[:changeset]
      assert user_params = u1_new
      assert html_response(conn, 302) =~ user_path(conn, :edit)

      u1_new = Users.get_user_by(id: u1.id)

      conn =
        conn
        |> recycle()
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> get(user_path(conn, :edit))

      assert conn.assigns.user.email == user_params.email
    end

    test "of bigquery_project_id resets all user tables", %{
      conn: conn,
      users: [u1 | _]
    } do
      expect(BigQuery, :create_dataset, fn _, _, _, _ -> {:ok, []} end)
      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> put(
          "/account/edit",
          %{
            "user" => %{
              "bigquery_project_id" => "logflare-byob-for-test"
            }
          }
        )

      refute conn.assigns[:changeset]
      assert redirected_to(conn, 302) =~ user_path(conn, :edit)
    end
  end

  describe "UserController delete" do
    test "succeeds", %{conn: conn, users: [u1 | _]} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> delete(user_path(conn, :delete))

      u1_updated = Users.get_user_by(id: u1.id)
      refute u1_updated
      assert redirected_to(conn, 302) == auth_path(conn, :login, user_deleted: true)
    end
  end
end
