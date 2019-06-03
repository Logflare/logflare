defmodule LogflareWeb.RuleControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.{SystemCounter, Sources, Repo}
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

  describe "RuleController create" do
    test "succeeds for authorized user", %{
      conn: conn,
      users: [u1, u2],
      sources: [u1s1, u1s2, u2s1 | _]
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> assign(:source, u1s1)
        |> post(
          source_rule_path(conn, :create, u1s1.id),
          %{
            "rule" => %{
              regex: "\| 4.. \| ",
              sink: u1s2.token
            }
          }
        )

      rules_db = Sources.get_by(id: u1s1.id).rules

      assert length(rules_db) == 1
      assert hd(rules_db) == %{regex: "\| 4.. \| "}
      assert get_flash(conn, :info) === "Rule created successfully!"
      assert redirected_to(source_rule_path(conn, :index, u1s1.id), 302)

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
end
