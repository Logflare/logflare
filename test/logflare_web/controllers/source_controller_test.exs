defmodule LogflareWeb.SourceControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase

  alias Logflare.{SourceManager, SystemCounter, Sources, Repo, Users}
  alias Logflare.Logs.RejectedEvents
  import Logflare.DummyFactory

  setup do
    s1 = insert(:source, token: Faker.UUID.v4())
    s2 = insert(:source, token: Faker.UUID.v4())
    s3 = insert(:source, token: Faker.UUID.v4())
    u1 = insert(:user, sources: [s1, s2])
    u2 = insert(:user, sources: [s3])

    users = Repo.preload([u1, u2], :sources)

    sources = [s1, s2, s3]

    SystemCounter.start_link()
    {:ok, users: users, sources: sources, conn: Phoenix.ConnTest.build_conn()}
  end

  describe "dashboard" do
    test "renders dashboard", %{conn: conn, users: [u1, u2], sources: [s1, s2 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get("/dashboard")

      dash_sources = Enum.map(conn.assigns.sources, & &1.metrics)
      dash_source_1 = hd(dash_sources)

      source_stat_fields = ~w[avg buffer inserts latest max rate id]a

      assert is_list(dash_sources)
      assert source_stat_fields -- Map.keys(dash_source_1) === []
      assert hd(conn.assigns.sources).id == s1.id
      assert hd(conn.assigns.sources).token == s1.token
      assert html_response(conn, 200) =~ "dashboard"
    end

    test "renders rejected logs page", %{conn: conn, users: [u1, u2], sources: [s1, s2 | _]} do
      RejectedEvents.injest(%{
        error: Logflare.Validator.DeepFieldTypes,
        batch: [%{"no_log_entry" => true, "timestamp" => ""}],
        source: s1
      })

      conn =
        conn
        |> login_user(u1)
        |> get("/sources/#{s1.id}/rejected")

      assert html_response(conn, 200) =~ "dashboard"

      assert [
               %{
                 message:
                   "Metadata validation error: values with the same field path must have the same type.",
                 payload: [%{"no_log_entry" => true, "timestamp" => ""}],
                 timestamp: _
               }
             ] = conn.assigns.logs
    end

    test "update with valid params", %{conn: conn, users: [u1, u2], sources: [s1, s2 | _]} do
      new_name = Faker.String.base64()

      params = %{
        "id" => s1.id,
        "source" => %{
          "favorite" => true,
          "name" => new_name
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch("/sources/#{s1.id}", params)

      s1_new = Sources.get_by_id(s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert get_flash(conn, :info) == "Source updated!"
      assert s1_new.name == new_name
      assert s1_new.favorite == true
    end

    @tag :skip
    test "update action with invalid params", %{conn: conn, users: [u1, u2], sources: [s1, s2]} do
      new_name = "this should never be inserted"

      params = %{
        "id" => s1.id,
        "source" => %{
          "favorite" => 1,
          "name" => new_name
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch("/sources/#{s1.id}", params)

      s1_new = Sources.get_by_id(s1.token)

      assert s1_new.name != new_name
      assert get_flash(conn, :error) == "Something went wrong!"
      assert html_response(conn, 406) =~ "Source Name"
    end

    test "users can't update restricted fields", %{
      conn: conn,
      users: [u1, u2],
      sources: [s1, s2 | _]
    } do
      nope_token = Faker.UUID.v4()
      nope_api_quota = 1337
      nope_user_id = 1

      params = %{
        "id" => s1.id,
        "source" => %{
          "name" => s1.name,
          "token" => nope_token,
          "api_quota" => nope_api_quota,
          "user_id" => nope_user_id
        }
      }

      conn =
        conn
        |> login_user(u1)
        |> patch("/sources/#{s1.id}", params)

      s1_new = Sources.get_by_pk(s1.id)

      refute conn.assigns[:changeset]
      refute s1_new.token == nope_token
      refute s1_new.api_quota == nope_api_quota
      refute s1_new.user_id == nope_user_id
      assert conn.status == 200
      assert html_response(conn, 401) =~ "Not allowed"
    end

    test "users can't update sources of other users", %{
      conn: conn,
      users: [u1, u2],
      sources: [s1, s2, u2s1]
    } do
      conn =
        conn
        |> login_user(u1)
        |> patch(
          "/sources/#{u2s1.id}",
          %{
            "source" => %{
              "name" => "it's mine now!"
            }
          }
        )

      s1_new = Sources.get_by_pk(s1.id)

      refute s1_new.name == "it's mine now!"
      assert conn.halted === true
      assert get_flash(conn, :error) =~ "That's not yours!"
      assert redirected_to(conn, 401) =~ "Not allowed"
    end
  end

  describe "create" do
    test "with valid params", %{conn: conn, users: [u1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> post("/sources", %{
          "source" => %{
            "name" => Faker.Name.name(),
            "token" => Faker.UUID.v4()
          }
        })

      refute conn.assigns[:changeset]
      assert redirected_to(conn, 302) === source_path(conn, :dashboard)
    end

    test "with invalid params", %{conn: conn, users: [u1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> post("/sources", %{
          "source" => %{
            "name" => Faker.Name.name()
            # "token" => Faker.UUID.v4()
          }
        })

      assert conn.assigns[:changeset].errors === [
               token: {"can't be blank", [validation: :required]}
             ]

      assert redirected_to(conn, 406) === source_path(conn, :new)
    end
  end

  def login_user(conn, u) do
    conn
    |> assign(:user, u)
  end
end
