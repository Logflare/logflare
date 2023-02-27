defmodule LogflareWeb.UserControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase

  alias Logflare.{Users}
  # alias Logflare.Source
  # alias Logflare.Google.BigQuery
  import Logflare.Factory
  @moduletag :failing

  setup do
    u1 = insert(:user, bigquery_dataset_id: "test_dataset_id_1")
    u2 = insert(:user, bigquery_dataset_id: "test_dataset_id_2")
    # allow Users.Cache.get_by(any()), return: :should_not_happen

    {:ok, users: [u1, u2], conn: Phoenix.ConnTest.build_conn()}
  end

  describe "UserController update" do
    test "of restricted fields fails", %{
      conn: conn,
      users: [u1 | _]
    } do
      nope_token = TestUtils.random_string()
      nope_api_quota = 1337
      nope_user_id = 1

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> put(
          "/account/edit",
          %{
            "user" => %{
              "name" => u1.name,
              "token" => nope_token,
              "api_quota" => nope_api_quota,
              "id" => nope_user_id
            }
          }
        )

      s1_new = Users.get_by(id: u1.id)

      refute conn.assigns[:changeset]
      refute s1_new.token == nope_token
      refute s1_new.api_quota == nope_api_quota
      refute s1_new.id == nope_user_id
      assert redirected_to(conn, 302) =~ user_path(conn, :edit)
      # refute_called(Users.Cache.get_by(any()), once())
    end

    test "of allowed fields succeeds", %{
      conn: conn,
      users: [u1 | _]
    } do
      new_email = TestUtils.gen_email()

      new = %{
        email: new_email,
        provider: TestUtils.random_string(),
        email_preferred: TestUtils.gen_email(),
        name: TestUtils.random_string(),
        image: "https://#{TestUtils.random_string()}.com",
        email_me_product: true,
        phone: 12345
      }

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> put("/account/edit", %{"user" => new})

      s1_new =
        Users.get_by(id: u1.id)
        |> Map.from_struct()
        |> Map.take(Map.keys(new))

      refute conn.assigns[:changeset]
      assert s1_new == new
      assert html_response(conn, 302) =~ user_path(conn, :edit)

      conn =
        conn
        |> recycle()
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> get(user_path(conn, :edit))

      assert conn.assigns.user.email == new_email

      # refute_called(Users.Cache.get_by(any()), once())
    end

    test "of bigquery_project_id resets all user tables", %{
      conn: conn,
      users: [u1 | _]
    } do
      # allow BigQuery.create_dataset(any(), any(), any(), any()), return: {:ok, []}
      # allow Source.Supervisor.reset_all_user_sources(any()), return: :ok

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
      # assert get_flash(conn, :new_bq_project)
    end
  end

  describe "UserController delete" do
    test "succeeds", %{conn: conn, users: [u1 | _]} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u1.id})
        |> delete(user_path(conn, :delete))

      u1_updated = Users.get_by(id: u1.id)
      refute u1_updated
      assert redirected_to(conn, 302) == auth_path(conn, :login, user_deleted: true)
      # refute_called(Users.Cache.get_by(any()), once())
    end
  end
end
