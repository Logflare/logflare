defmodule LogflareWeb.RuleControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.{SystemCounter, Sources, Repo}
  import Logflare.DummyFactory

  setup do
    u1 = insert(:user)
    u2 = insert(:user)

    s1 = insert(:source, public_token: Faker.String.base64(16), user_id: u1.id)
    s2 = insert(:source, user_id: u1.id)
    s3 = insert(:source, user_id: u2.id)

    users = Repo.preload([u1, u2], :sources)

    sources = [s1, s2, s3]

    {:ok, users: users, sources: sources}
  end

  describe "RuleController create" do
    test "succeeds for authorized user", %{
      conn: conn,
      users: [u1, u2],
      sources: [u1s1, u1s2, u2s1 | _]
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> post(
          source_rule_path(conn, :create, u1s1.id),
          %{
            "rule" => %{
              regex: "\| 4.. \| ",
              sink: u1s2.token
            }
          }
        )

      rules = Sources.get_by(id: u1s1.id).rules

      assert length(rules) == 1
      assert hd(rules).regex == "\| 4.. \| "
      assert %Regex{source: "\| 4.. \| "} = hd(rules).regex_struct
      assert get_flash(conn, :info) === "Rule created successfully!"
      assert redirected_to(conn, 302) == source_rule_path(conn, :index, u1s1.id)
    end

    test "fails for invalid regex", %{
      conn: conn,
      users: [u1, _u2],
      sources: [u1s1, u1s2 | _]
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> post(
          source_rule_path(conn, :create, u1s1.id),
          %{
            "rule" => %{
              regex: "*Googlebot",
              sink: u1s2.token
            }
          }
        )

      rules_db = Sources.get_by(id: u1s1.id).rules

      assert get_flash(conn, :error) === "regex: nothing to repeat at position 0\n"

      conn =
        conn
        |> recycle()
        |> assign(:user, u1)
        |> post(
          source_rule_path(conn, :create, u1s1.id),
          %{
            "rule" => %{
              regex: "",
              sink: u1s2.token
            }
          }
        )

      rules_db = Sources.get_by(id: u1s1.id).rules

      assert get_flash(conn, :error) === "regex: can't be blank\n"
    end

    test "fails for unauthorized user", %{
      conn: conn,
      users: [u1, u2 | _],
      sources: [u1s1, u1s2, u2s1 | _]
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> post(source_rule_path(conn, :create, u2s1.id), %{
          "rule" => %{
            regex: "\| 4.. \| ",
            sink: u1s2.token
          }
        })

      rules_db = Sources.get_by(id: u1s1.id).rules

      assert rules_db == []
      assert get_flash(conn, :error) == "That's not yours!"
      assert redirected_to(conn, 403) == "/"
    end
  end

  describe "RuleController index" do
    test "succeeds for authorized user", %{
      conn: conn,
      users: [u1, u2],
      sources: [u1s1, u1s2, u2s1 | _]
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> get(source_rule_path(conn, :index, u1s1.id))

      assert html_response(conn, 200) =~ "Rules"
    end

    test "fails for unauthorized user", %{
      conn: conn,
      users: [u1, u2 | _],
      sources: [u1s1, u1s2, u2s1 | _]
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> get(source_rule_path(conn, :index, u2s1.id))

      assert redirected_to(conn, 403) == "/"
    end
  end
end
