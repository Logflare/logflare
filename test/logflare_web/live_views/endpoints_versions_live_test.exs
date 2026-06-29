defmodule LogflareWeb.EndpointsVersionsLiveTest do
  @moduledoc false

  use LogflareWeb.ConnCase

  alias Logflare.Endpoints
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
      endpoint: endpoint,
      team: team
    } do
      {:ok, view, html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      assert html =~ "No versions recorded for this endpoint."

      refute has_element?(
               view,
               "#endpoint-versions div[id^='versions-'] button[phx-click='show-version']"
             )
    end

    test "omits relative timestamp when the version is older than a week", %{endpoint: endpoint} do
      inserted_at =
        DateTime.utc_now()
        |> DateTime.add(-8, :day)
        |> DateTime.truncate(:second)

      current_version =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 2,
          item_changes: %{"description" => "current version"},
          snapshot_overrides: %{"description" => "current version"}
        )

      version =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 1,
          inserted_at: inserted_at,
          item_changes: %{"description" => "old version"},
          snapshot_overrides: %{"description" => "old version"}
        )

      version_html =
        render_component(&LogflareWeb.EndpointsVersionsLive.version_updated/1,
          version: version,
          current_version_id: current_version.id
        )

      date = Calendar.strftime(inserted_at, "%Y-%m-%d")

      assert version_html =~ Calendar.strftime(inserted_at, "%Y-%m-%d %H:%M:%S UTC")
      assert length(String.split(version_html, date)) - 1 == 1
      assert version_html =~ "Restore"
    end

    test "lists versions, newest first", %{
      conn: conn,
      user: user,
      endpoint: endpoint,
      team: team
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

      {:ok, view, html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      version_3_html = view |> element(endpoint_version_row(version_3)) |> render()

      assert version_3_html =~ "current"
      assert version_3_html =~ "Just now"
      assert version_3_html =~ "latest description"
      assert version_3_html =~ "from endpoint_versions "
      assert html =~ user.email

      version_2_html = view |> element(endpoint_version_row(version_2)) |> render()

      assert version_2_html =~ "Caching:"
      assert version_2_html =~ "120 seconds"
      assert version_2_html =~ "Authentication:"
      assert version_2_html =~ "disabled"
      assert version_2_html =~ user.email

      version_1_html = view |> element(endpoint_version_row(version_1)) |> render()

      assert version_1_html =~ "Description:"
      assert version_1_html =~ "created description"
      assert version_1_html =~ user.email
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

      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      view
      |> element(endpoint_version_row_button(version))
      |> render_click()

      patched_uri = assert_patch(view) |> URI.parse()

      assert "/endpoints/#{endpoint.id}/versions" == patched_uri.path
      assert "t=#{team.id}&version_number=2" == patched_uri.query

      modal_html =
        view
        |> element("#endpoint-version-snapshot-modal")
        |> render()

      assert modal_html =~ "Version 2"
      assert modal_html =~ "BigQuery SQL"
      assert modal_html =~ "copy"
      assert modal_html =~ "select"
      assert modal_html =~ "2 as version_number"
      assert modal_html =~ "snapshot description"
      assert_query_displayed(view, "select 2 as version_number")

      view
      |> element("#endpoint-version-snapshot-modal .phx-modal-close")
      |> render_click()

      patched_uri = assert_patch(view) |> URI.parse()

      assert "/endpoints/#{endpoint.id}/versions" == patched_uri.path
      assert "t=#{team.id}" == patched_uri.query
      refute has_element?(view, "#endpoint-version-snapshot-modal")
    end

    test "loads additional versions", %{
      conn: conn,
      endpoint: endpoint,
      team: team,
      user: user
    } do
      versions =
        for version_number <- 1..26 do
          insert(:endpoint_version,
            endpoint: endpoint,
            version_number: version_number,
            origin: user.email,
            item_changes: %{"description" => "version-#{version_number}-description"},
            snapshot_overrides: %{"description" => "version-#{version_number}-description"}
          )
        end

      {:ok, view, html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      assert html =~ "version-26-description"
      assert html =~ "version-2-description"
      refute html =~ "version-1-description"
      assert has_element?(view, "button", "Load more")

      loading_html =
        view
        |> element("button", "Load more")
        |> render_click()

      assert loading_html =~ "Loading..."

      html = render_async(view)

      assert html =~ "version-1-description"
      assert html =~ ~s|id="versions-#{List.first(versions).id}"|
      refute has_element?(view, "button", "Load more")
    end

    test "restores a historical version by appending a new current version", %{
      conn: conn,
      endpoint: endpoint,
      team: team,
      user: user
    } do
      version_1 =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 1,
          origin: user.email,
          item_changes: %{"description" => "first description"},
          snapshot_overrides: %{
            "description" => "first description",
            "query" => "select 1 as restored_version"
          }
        )

      version_2 =
        insert(:endpoint_version,
          endpoint: endpoint,
          version_number: 2,
          origin: user.email,
          item_changes: %{"description" => "second description"},
          snapshot_overrides: %{
            "description" => "second description",
            "query" => "select 2 as current_version"
          }
        )

      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      assert has_element?(view, version_restore_button(version_1), "Restore")
      refute has_element?(view, version_restore_button(version_2), "Restore")

      view
      |> element(version_restore_button(version_1), "Restore")
      |> render_click()

      assert render_async(view) =~ "Restored endpoint version 1 as new version 3."

      restored_endpoint = Endpoints.get_endpoint_query(endpoint.id)

      assert restored_endpoint.description == "first description"
      assert restored_endpoint.query == "select 1 as restored_version"

      [
        %Version{
          meta: %{
            "version_number" => 3,
            "endpoint_snapshot" => %{"description" => "first description"}
          },
          origin: latest_version_origin
        } = latest_version
        | _versions
      ] =
        PaperTrail.get_versions(Endpoints.EndpointQuery, endpoint.id)
        |> Enum.sort_by(& &1.id, :desc)

      assert latest_version_origin == user.email

      latest_version_html = view |> element(endpoint_version_row(latest_version)) |> render()

      assert latest_version_html =~ "current"
      assert latest_version_html =~ "first description"
      refute has_element?(view, version_restore_button(latest_version), "Restore")
    end

    test "restore errors are displayed", %{conn: conn, endpoint: endpoint, team: team} do
      {:ok, view, _html} = live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      assert render_click(view, "restore-version", %{"version-number" => "abc"}) =~
               "Unable to restore endpoint version."

      assert Endpoints.get_endpoint_query(endpoint.id).query == endpoint.query
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

    test "team user is redirected to versions with team param end links preserve resolved team",
         %{
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
        |> live(~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      assert html =~ endpoint.name
      assert html =~ "team-visible-version"
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
        |> live(~p"/endpoints/#{endpoint.id}/edit?t=#{team}")

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
        |> live(~p"/endpoints/#{endpoint.id}/versions?t=#{team}")

      assert html =~ "updated by team user"
      assert html =~ team_user.email
    end

    test "ignores malformed and missing version query params", %{
      conn: conn,
      endpoint: endpoint,
      team: team
    } do
      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        item_changes: %{"description" => "existing version"},
        snapshot_overrides: %{"description" => "existing version"}
      )

      {:ok, view, _html} =
        live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}&version_number=abc")

      refute has_element?(view, "#endpoint-version-snapshot-modal")

      {:ok, view, _html} =
        live(conn, ~p"/endpoints/#{endpoint.id}/versions?t=#{team}&version_number=999")

      refute has_element?(view, "#endpoint-version-snapshot-modal")
    end
  end

  defp endpoint_version_row(%Version{id: version_id}), do: "#versions-#{version_id}"

  defp endpoint_version_row_button(%Version{} = version),
    do: "#{endpoint_version_row(version)} button[phx-click='show-version']"

  defp version_restore_button(%Version{} = version),
    do: "#{endpoint_version_row(version)} button[phx-click][phx-value-version-number]"

  defp assert_query_displayed(view, query) do
    {:ok, formatted_query} = Logflare.Sql.format(query)

    assert has_element?(view, "code", formatted_query)
  end
end
