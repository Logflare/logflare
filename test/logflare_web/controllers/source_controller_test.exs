defmodule LogflareWeb.SourceControllerTest do
  @moduledoc false
  import LogflareWeb.Router.Helpers
  use LogflareWeb.ConnCase

  alias Logflare.{SystemCounter, Sources, Repo}
  alias Logflare.Logs.RejectedEvents
  import Logflare.DummyFactory

  setup do
    s1 = insert(:source, public_token: Faker.String.base64(16))
    s2 = insert(:source)
    s3 = insert(:source)
    u1 = insert(:user, sources: [s1, s2])
    u2 = insert(:user, sources: [s3])

    users = Repo.preload([u1, u2], :sources)

    sources = [s1, s2, s3]

    {:ok, users: users, sources: sources}
  end

  describe "dashboard" do
    test "renders dashboard", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
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

    test "renders rejected logs page", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
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
  end

  describe "update" do
    test "returns 200 with valid params", %{conn: conn, users: [u1, _u2], sources: [s1, _s2 | _]} do
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

      s1_new = Sources.get_by(token: s1.token)

      assert html_response(conn, 302) =~ "redirected"
      assert get_flash(conn, :info) == "Source updated!"
      assert s1_new.name == new_name
      assert s1_new.favorite == true

      conn = conn
        |> recycle()
        |> login_user(u1)
        |> get(source_path(conn, :edit, s1.id))

      assert conn.assigns.source.name == new_name
    end

    test "returns 406 with invalid params", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2 | _]
    } do
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

      s1_new = Sources.get_by(token: s1.token)

      assert s1_new.name != new_name
      assert get_flash(conn, :error) == "Something went wrong!"
      assert html_response(conn, 406) =~ "Source Name"
    end

    test "", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2 | _]
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

      s1_new = Sources.get_by(id: s1.id)

      refute conn.assigns[:changeset]
      refute s1_new.token == nope_token
      refute s1_new.api_quota == nope_api_quota
      refute s1_new.user_id == nope_user_id
      assert redirected_to(conn, 302) =~ source_path(conn, :edit, s1.id)
    end

    test "returns 403 when user is not an owner of source", %{
      conn: conn,
      users: [u1, _u2],
      sources: [s1, _s2, u2s1 | _]
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

      s1_new = Sources.get_by(id: s1.id)

      refute s1_new.name === "it's mine now!"
      assert conn.halted === true
      assert get_flash(conn, :error) =~ "That's not yours!"
      assert redirected_to(conn, 403) =~ marketing_path(conn, :index)
    end
  end

  describe "show" do
    test "renders source for a logged in user", %{conn: conn, users: [u1 | _], sources: [s1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get(source_path(conn, :show, s1.id), %{
          "source" => %{
            "name" => Faker.Name.name()
          }
        })

      assert html_response(conn, 200) =~ s1.name
    end

    test "returns 403 for a source not owned by the user", %{
      conn: conn,
      users: [u1, u2 | _],
      sources: [s1 | _]
    } do
      conn =
        conn
        |> login_user(u2)
        |> get(source_path(conn, :show, s1.id))

      assert redirected_to(conn, 403) === "/"
    end
  end

  describe "create" do
    test "returns 200 with valid params", %{conn: conn, users: [u1 | _]} do
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

    test "returns 406 with invalid params", %{conn: conn, users: [u1 | _]} do
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

  describe "favorite" do
    test "returns 200 flipping the value", %{conn: conn, users: [u1 | _], sources: [s1 | _]} do
      conn =
        conn
        |> login_user(u1)
        |> get(source_path(conn, :favorite, Integer.to_string(s1.id)))

      new_s1 = Sources.get_by(id: s1.id)

      assert get_flash(conn, :info) == "Source updated!"
      assert redirected_to(conn, 302) =~ source_path(conn, :dashboard)
      assert new_s1.favorite == not s1.favorite
    end
  end

  describe "public" do
    test "shows a source page", %{conn: conn, sources: [s1 | _]} do
      conn =
        conn
        |> get(source_path(conn, :public, s1.public_token))

      assert html_response(conn, 200) =~ s1.name
    end
  end

  def login_user(conn, u) do
    conn
    |> assign(:user, u)
  end
end
