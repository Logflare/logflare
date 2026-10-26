defmodule LogflareWeb.BackendsDeletedBackendTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends
  alias Logflare.Repo

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    backend = insert(:backend, user: user, type: :incidentio)
    %{conn: login_user(conn, user), user: user, backend: backend}
  end

  test "saving an edit after another tab deletes the backend keeps the view alive", context do
    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    Repo.delete!(context.backend)

    html = render_hook(view, "save_backend", %{"backend" => %{"name" => "renamed"}})
    assert html =~ "Encountered error when updating backend"
    assert html =~ "Backend no longer exists"
    assert Process.alive?(view.pid)
  end

  test "setting default ingest on a deleted backend shows an error", context do
    backend = insert(:backend, user: context.user, type: :clickhouse)
    source = insert(:source, user: context.user, default_ingest_backend_enabled?: true)
    {:ok, view, _html} = live_with_redirect(context.conn, ~p"/backends/#{backend.id}")
    Repo.delete!(backend)

    html =
      render_hook(view, "save_default_ingest", %{
        "default_ingest" => %{"source_id" => to_string(source.id)}
      })

    assert html =~ "Error setting default ingest"
    assert html =~ "Backend no longer exists"
    assert Process.alive?(view.pid)
  end

  test "adding an alert to a deleted backend shows an error", context do
    alert = insert(:alert, user: context.user)
    {:ok, view, _html} = live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}")
    Repo.delete!(context.backend)

    html = render_hook(view, "add_alert", %{"alert" => %{"alert_id" => to_string(alert.id)}})
    assert html =~ "Encountered error when adding alert"
    assert html =~ "Backend no longer exists"
    assert Process.alive?(view.pid)
  end

  test "removing an alert from a deleted backend shows an error", context do
    alert = insert(:alert, user: context.user)
    assert {:ok, backend} = Backends.update_backend(context.backend, %{alert_queries: [alert]})
    {:ok, view, _html} = live_with_redirect(context.conn, ~p"/backends/#{backend.id}")
    Repo.delete!(backend)

    html = render_hook(view, "remove_alert", %{"alert_id" => to_string(alert.id)})
    assert html =~ "Encountered error when removing alert"
    assert html =~ "Backend no longer exists"
    assert Process.alive?(view.pid)
  end
end
