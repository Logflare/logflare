defmodule LogflareWeb.EndpointsVersionsLiveTest do
  @moduledoc false

  use LogflareWeb.ConnCase

  alias PaperTrail.Version

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    endpoint = insert(:endpoint, user: user)

    conn = login_user(conn, user)

    {:ok, user: user, team: team, conn: conn, endpoint: endpoint}
  end

  describe "endpoint versions" do
    test "empty state when endpoint has no versions", %{
      conn: conn,
      endpoint: endpoint
    } do
      {:ok, view, html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions")

      assert html =~ "No versions recorded for this endpoint."
      refute has_element?(view, "#endpoint-versions a[id^='versions-']")
    end

    test "lists versions, newest first", %{
      conn: conn,
      user: user,
      endpoint: endpoint
    } do
      version_1 =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 1,
          origin: user.email,
          item_changes: %{"description" => "created description"},
          snapshot_overrides: %{"description" => "created description"}
        )

      version_2 =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 2,
          origin: user.email,
          item_changes: %{"cache_duration_seconds" => 120, "enable_auth" => false},
          snapshot_overrides: %{"cache_duration_seconds" => 120, "enable_auth" => false}
        )

      version_3 =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 3,
          origin: user.email,
          item_changes: %{
            "description" => "latest description",
            "query" => "select updated_value from production_log"
          },
          snapshot_overrides: %{
            "description" => "latest description",
            "query" => "select updated_value from endpoint_versions"
          },
          meta: %{
            "query_diff" => [
              %{"type" => "eq", "value" => "select "},
              %{"type" => "del", "value" => "current_date() "},
              %{"type" => "ins", "value" => "updated_value "},
              %{"type" => "eq", "value" => "from endpoint_versions "}
            ]
          }
        )

      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions")

      assert view |> element(endpoint_version_row(version_3)) |> render() |> normalized_text() =~
               normalized_text("""
               Version
               current
               3
               Changes
               Description:
               latest description
               Query:select current_date() updated_value from endpoint_versions
               Author
               #{user.email}
               Updated
               #{Calendar.strftime(version_3.inserted_at, "%Y-%m-%d %H:%M:%S UTC")}
               """)

      assert view |> element(endpoint_version_row(version_2)) |> render() |> normalized_text() =~
               normalized_text("""
               Version
               2
               Changes
               Caching:
               120 seconds
               Authentication:
               disabled
               Author
               #{user.email}
               Updated
               #{Calendar.strftime(version_2.inserted_at, "%Y-%m-%d %H:%M:%S UTC")}
               """)

      assert view |> element(endpoint_version_row(version_1)) |> render() |> normalized_text() =~
               normalized_text("""
               Version
               1
               Changes
               Description:
               created description
               Author
               #{user.email}
               Updated
               #{Calendar.strftime(version_1.inserted_at, "%Y-%m-%d %H:%M:%S UTC")}
               """)
    end

    test "snapshot modal", %{
      conn: conn,
      endpoint: endpoint,
      team: team,
      user: user
    } do
      version =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 2,
          origin: user.email,
          item_changes: %{"description" => "snapshot description"},
          snapshot_overrides: %{
            "description" => "snapshot description",
            "query" => "select 2 as version_number",
            "enable_auth" => false,
            "max_limit" => 250,
            "cache_duration_seconds" => 120
          }
        )

      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions")

      view
      |> element(endpoint_version_row(version))
      |> render_click()

      patched_uri = assert_patch(view) |> URI.parse()

      assert "/endpoints/#{endpoint.id}/versions" == patched_uri.path
      assert "t=#{team.id}&version_number=2" == patched_uri.query

      text =
        view
        |> element("#endpoint-version-snapshot-modal")
        |> render()
        |> normalized_text()

      assert text =~
               normalized_text("""
                 Version 2
                 Authentication:disabledMax rows:250Caching:120 secondsCache warming:1800 secondsQuery sandboxing:disabledRedact PII:disabledDynamic reservation:disabled
                 BigQuery SQL
                 copy
                 SELECT
                 2 AS version_number
               """)

      assert text =~ "snapshot description"
      assert_query_displayed(view, "select 2 as version_number")
    end

    test "loads additional versions", %{
      conn: conn,
      endpoint: endpoint,
      user: user
    } do
      for version_number <- 1..26 do
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: version_number,
          origin: user.email,
          item_changes: %{"description" => "version-#{version_number}-description"},
          snapshot_overrides: %{"description" => "version-#{version_number}-description"}
        )
      end

      {:ok, view, html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions")

      assert html =~ "version-26-description"
      assert html =~ "version-2-description"
      refute html =~ "version-1-description"
      assert has_element?(view, "button", "Load more")

      view
      |> element("button", "Load more")
      |> render_click()

      html = render(view)

      assert html =~ "version-1-description"
      refute has_element?(view, "button", "Load more")
    end

    test "redirects when user cannot access endpoint versions", %{conn: conn} do
      victim = insert(:user)
      victim_endpoint = insert(:endpoint, user: victim)

      insert(:endpoint_version,
        endpoint: victim_endpoint,
        version_number: 1,
        item_changes: %{"description" => "victim-only-version"},
        snapshot_overrides: %{"description" => "victim-only-version"}
      )

      assert {:error, {:redirect, %{to: "/endpoints"}}} =
               live(conn, ~p"/endpoints/#{victim_endpoint.id}/versions")
    end

    test "team user can view versions without team param and links preserve resolved team", %{
      conn: conn,
      endpoint: endpoint,
      team: team,
      user: user
    } do
      team_user = insert(:team_user, team: team)

      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        origin: team_user.email,
        item_changes: %{"description" => "team-visible-version"},
        snapshot_overrides: %{"description" => "team-visible-version"}
      )

      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/endpoints/#{endpoint.id}/versions")

      text = normalized_text(html)

      assert text =~ endpoint.name
      assert text =~ "team-visible-version"
      assert html =~ ~s|href="/endpoints?t=#{team.id}"|
      assert html =~ ~s|href="/endpoints/#{endpoint.id}?t=#{team.id}"|
    end

    test "team user edit creates version with team user author", %{
      conn: conn,
      endpoint: endpoint,
      team: team,
      user: user
    } do
      team_user = insert(:team_user, team: team)

      {:ok, view, _html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/endpoints/#{endpoint.id}/edit?t=#{team.id}")

      view
      |> element("form#endpoint")
      |> render_submit(%{
        endpoint: %{
          description: "updated by team user"
        }
      })

      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/endpoints/#{endpoint.id}/versions?t=#{team.id}")

      text = normalized_text(html)

      assert text =~ "updated by team user"
      assert text =~ team_user.email
    end

    test "ignores malformed and missing version query params", %{
      conn: conn,
      endpoint: endpoint
    } do
      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        item_changes: %{"description" => "existing version"},
        snapshot_overrides: %{"description" => "existing version"}
      )

      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?version_number=abc")
      refute has_element?(view, "#endpoint-version-snapshot-modal")

      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?version_number=999")
      refute has_element?(view, "#endpoint-version-snapshot-modal")
    end
  end

  defp endpoint_version_row(%Version{id: version_id}), do: "#versions-#{version_id}"

  defp normalized_text(html) do
    html
    |> Floki.parse_fragment!()
    |> Floki.text()
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
  end

  defp assert_query_displayed(view, query) do
    {:ok, formatted_query} = SqlFmt.format_query(query)

    assert has_element?(view, "code", formatted_query)
  end
end
