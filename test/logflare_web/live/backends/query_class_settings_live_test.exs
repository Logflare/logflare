defmodule LogflareWeb.Backends.QueryClassSettingsLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends
  alias Logflare.Repo

  @cache_key "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"

  setup %{conn: conn} do
    insert(:plan)
    owner = insert(:user, admin: false)
    team = insert(:team, user: owner)

    backend =
      insert(:backend,
        user: owner,
        type: :clickhouse,
        config: %{url: "http://localhost:8123", port: 8123, database: "default"}
      )

    %{conn: login_user(conn, owner), owner: owner, team: team, backend: backend}
  end

  test "owner saves defaults and class overrides independently of backend config", context do
    {:ok, view, html} = live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")
    assert html =~ "Query-class settings"
    assert html =~ "Updates propagate eventually"

    reject(Backends, :update_backend, 2)

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "5", "max_threads" => "8", "max_execution_time" => "2.5"},
        "api_paid" => %{"priority" => "10", "max_threads" => "4", "max_rows_to_read" => "1000"}
      }
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")
    saved = Backends.get_backend(context.backend.id)

    assert saved.config.query_class_settings == %{
             "default" => %{
               "priority" => 5,
               "max_threads" => 8,
               "max_execution_time" => 2.5,
               "timeout_overflow_mode" => "throw"
             },
             "api_paid" => %{
               "priority" => 10,
               "max_threads" => 4,
               "max_rows_to_read" => 1000,
               "read_overflow_mode" => "throw"
             }
           }

    assert saved.name == context.backend.name
    assert saved.config.url == "http://localhost:8123"

    {:ok, reloaded, _html} = live_with_redirect(context.conn, ~p"/backends/#{saved.id}/edit")
    assert has_element?(reloaded, "#{policy_input("default", "priority")}[value='5']")
    assert has_element?(reloaded, "#{policy_input("api_paid", "max_threads")}[value='4']")
  end

  test "invalid policy is rejected without losing entered values or stored policy", context do
    {:ok, _backend} =
      Backends.configure_query_class_settings(context.owner, context.backend, %{
        "default" => %{"priority" => 5}
      })

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "0"},
        "api_paid" => %{"max_threads" => "4"}
      }
    )
    |> render_submit()

    assert has_element?(view, "#query-class-settings-error")
    assert has_element?(view, "#{policy_input("default", "priority")}[value='0']")
    assert has_element?(view, "#{policy_input("api_paid", "max_threads")}[value='4']")

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5}
           }
  end

  test "malformed numeric fields are rejected on save and reset without losing the draft",
       context do
    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    editor = with_target(view, "#backend-query-class-settings-#{context.backend.id}")

    invalid_number = %{"query_class_settings" => %{"default" => %{"priority" => "4.5"}}}
    assert render_submit(editor, "save", invalid_number) =~ "Invalid enforced ClickHouse setting"

    malformed = %{
      "query_class_settings" => %{"default" => %{"priority" => %{"nested" => "5"}}}
    }

    assert render_submit(editor, "save", malformed) =~ "named classes and numeric fields"

    assert render_submit(editor, "save", Map.put(malformed, "reset_class", "api_paid")) =~
             "named classes and numeric fields"

    assert has_element?(view, "#{policy_input("default", "priority")}[value='4.5']")
    assert Backends.get_backend(context.backend.id).config[:query_class_settings] in [nil, %{}]

    valid = %{"query_class_settings" => %{"default" => %{"priority" => "5"}}}
    assert render_submit(editor, "save", valid) =~ "Query-class settings saved"
  end

  test "whole-number inputs are parsed exactly, including decimal and exponent notation",
       context do
    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{
          "priority" => "4.0",
          "max_threads" => "1e2",
          "max_bytes_to_read" => "9.223372036854775807e18"
        }
      }
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")

    assert Backends.get_backend(context.backend.id).config.query_class_settings["default"] == %{
             "priority" => 4,
             "max_threads" => 100,
             "max_bytes_to_read" => 9_223_372_036_854_775_807,
             "read_overflow_mode" => "throw"
           }
  end

  test "saved legacy timeouts are flagged until corrected", %{backend: backend, conn: conn} do
    policy = %{
      "default" => %{"priority" => 5, "max_execution_time" => 0.0000005},
      "api_paid" => %{"max_execution_time" => 2.5},
      "api_free" => %{"max_execution_time" => 10_000_000_000_000}
    }

    persisted = Repo.reload!(backend)

    persisted
    |> Ecto.Changeset.change(
      config_encrypted: Map.put(persisted.config_encrypted, "query_class_settings", policy)
    )
    |> Repo.update!()

    {:ok, view, _html} = live_with_redirect(conn, ~p"/backends/#{backend.id}/edit")

    for class <- ["default", "api_free"] do
      warning_id = "query-class-#{class}-timeout-warning"
      assert has_element?(view, "##{warning_id}", "Queries ignore this saved time limit")

      assert has_element?(
               view,
               "#{policy_input(class, "max_execution_time")}[aria-describedby='#{warning_id}']"
             )
    end

    refute has_element?(view, "#query-class-api_paid-timeout-warning")
    refute has_element?(view, "#query-class-mcp-timeout-warning")
    assert Backends.get_backend(backend.id).config.query_class_settings == policy

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "5", "max_execution_time" => "0.5"},
        "api_free" => %{"max_execution_time" => ""}
      }
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")
    refute has_element?(view, "#query-class-default-timeout-warning")
    refute has_element?(view, "#query-class-api_free-timeout-warning")
  end

  test "sub-microsecond timeouts cannot be saved through the editor", context do
    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "5", "max_execution_time" => ".0000005"}
      }
    )
    |> render_submit()

    assert has_element?(view, "#query-class-settings-error", "microsecond")
    assert Backends.get_backend(context.backend.id).config[:query_class_settings] in [nil, %{}]
  end

  test "blank overrides inherit defaults and disable clears only class policy", context do
    {:ok, _backend} =
      Backends.configure_query_class_settings(context.owner, context.backend, %{
        "default" => %{"priority" => 5},
        "api_paid" => %{"max_threads" => 4}
      })

    endpoint =
      insert(:endpoint,
        user: context.owner,
        backend: context.backend,
        language: :ch_sql,
        enforced_clickhouse_settings: %{"max_threads" => 2}
      )

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"api_paid" => %{"max_threads" => ""}}
    )
    |> render_submit()

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5}
           }

    view |> element("button", "Disable query-class policy") |> render_click()
    assert has_element?(view, "[role=status]", "Query-class policy disabled")
    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{}
    assert Repo.reload!(endpoint).enforced_clickhouse_settings == %{"max_threads" => 2}
  end

  test "a stale tab cannot restore an older limit or discard the draft", context do
    path = ~p"/backends/#{context.backend.id}/edit"

    assert {:ok, _} =
             Backends.configure_query_class_settings(context.owner, context.backend, %{
               "default" => %{"priority" => 5, "max_threads" => 12}
             })

    {:ok, first, _html} = live_with_redirect(context.conn, path)
    {:ok, second, _html} = live_with_redirect(context.conn, path)

    first
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"max_threads" => "4"}}
    )
    |> render_submit()

    assert has_element?(first, "[role=status]", "Query-class settings saved")

    second
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "6"}}
    )
    |> render_submit()

    assert has_element?(second, "#query-class-settings-error", "policy changed")
    assert has_element?(second, "#{policy_input("default", "priority")}[value='6']")

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5, "max_threads" => 4}
           }
  end

  test "a stale tab cannot disable a newly configured policy", context do
    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    policy = %{"default" => %{"priority" => 5}}

    assert {:ok, _} =
             Backends.configure_query_class_settings(context.owner, context.backend, policy)

    view |> element("button", "Disable query-class policy") |> render_click()

    assert has_element?(view, "#query-class-settings-error", "policy changed")
    assert Backends.get_backend(context.backend.id).config.query_class_settings == policy
  end

  test "ordinary edits preserve cache overrides and resetting a class removes only its overrides",
       context do
    policy = %{
      "default" => %{"priority" => 5, @cache_key => 0},
      "api_paid" => %{"max_threads" => 4, @cache_key => 1},
      "mcp" => %{"max_threads" => 2}
    }

    assert {:ok, _} =
             Backends.configure_query_class_settings(context.owner, context.backend, policy)

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    assert has_element?(view, "#query-class-api_paid-cache-override")
    refute has_element?(view, "input[name*='#{@cache_key}']")
    assert {:ok, _} = Backends.update_backend(context.backend, %{description: "Connection edit"})

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"api_paid" => %{"max_threads" => ""}}
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")
    assert has_element?(view, "#query-class-api_paid-cache-override")
    saved = put_in(policy["api_paid"], %{@cache_key => 1})
    assert Backends.get_backend(context.backend.id).config.query_class_settings == saved

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "6"}}
    )
    |> put_submitter("#query-class-api_paid button[name=reset_class]")
    |> render_submit()

    refute has_element?(view, "#query-class-api_paid-cache-override")
    assert has_element?(view, "#{policy_input("default", "priority")}[value='6']")
    assert Backends.get_backend(context.backend.id).config.query_class_settings == saved

    selector = policy_input("api_paid", "max_threads")
    [id] = view |> render() |> Floki.parse_fragment!() |> Floki.attribute(selector, "id")

    view |> form("#query-class-settings-form") |> render_submit()

    assert has_element?(view, "#{selector}[id='#{id}']")
    updated = Backends.get_backend(context.backend.id)
    assert updated.description == "Connection edit"

    assert updated.config.query_class_settings == %{
             "default" => %{"priority" => 6, @cache_key => 0},
             "mcp" => %{"max_threads" => 2}
           }
  end

  test "ordinary edits keep overflow modes even after the associated numeric limits are cleared",
       context do
    policy = %{
      "default" => %{"priority" => 5, "read_overflow_mode" => "throw"},
      "api_paid" => %{"max_rows_to_read" => 10, "timeout_overflow_mode" => "throw"},
      "mcp" => %{"max_execution_time" => 2.5}
    }

    assert {:ok, _} =
             Backends.configure_query_class_settings(context.owner, context.backend, policy)

    {:ok, view, _html} =
      live_with_redirect(context.conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"priority" => "6", "max_rows_to_read" => "100"}
      }
    )
    |> render_submit()

    defaults = %{"priority" => 6, "read_overflow_mode" => "throw"}

    assert Backends.get_backend(context.backend.id).config.query_class_settings["default"] ==
             Map.put(defaults, "max_rows_to_read", 100)

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{
        "default" => %{"max_rows_to_read" => ""},
        "api_paid" => %{"max_rows_to_read" => ""},
        "mcp" => %{"max_execution_time" => ""}
      }
    )
    |> render_submit()

    mcp = %{"timeout_overflow_mode" => "throw"}

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => defaults,
             "api_paid" => %{"read_overflow_mode" => "throw", "timeout_overflow_mode" => "throw"},
             "mcp" => mcp
           }

    view
    |> form("#query-class-settings-form")
    |> put_submitter("#query-class-api_paid button[name=reset_class]")
    |> render_submit()

    view |> form("#query-class-settings-form") |> render_submit()

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => defaults,
             "mcp" => mcp
           }

    view |> element("button", "Disable query-class policy") |> render_click()
    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{}
  end

  test "team members can edit, but revoked membership cannot save through an open view",
       context do
    member = insert(:team_user, team: context.team)
    conn = login_user(context.conn, context.owner, member)
    {:ok, view, _html} = live_with_redirect(conn, ~p"/backends/#{context.backend.id}/edit")

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "5"}}
    )
    |> render_submit()

    assert has_element?(view, "[role=status]", "Query-class settings saved")
    Repo.delete!(member)

    view
    |> form("#query-class-settings-form",
      query_class_settings: %{"default" => %{"priority" => "10"}}
    )
    |> render_submit()

    assert has_element?(view, "#query-class-settings-error", "You no longer have permission")

    assert Backends.get_backend(context.backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5}
           }
  end

  test "another account cannot open the editor", context do
    outsider = insert(:user)
    insert(:team, user: outsider)

    assert_raise LogflareWeb.ErrorsLive.InvalidResourceError, fn ->
      context.conn
      |> login_user(outsider)
      |> live_with_redirect(~p"/backends/#{context.backend.id}/edit")
    end
  end

  test "non-ClickHouse backends do not display the editor", context do
    backend = insert(:backend, user: context.owner, type: :bigquery)
    {:ok, view, _html} = live_with_redirect(context.conn, ~p"/backends/#{backend.id}/edit")
    refute has_element?(view, "#query-class-settings-form")
  end

  @spec policy_input(String.t(), String.t()) :: String.t()
  defp policy_input(class, key), do: "input[name='query_class_settings[#{class}][#{key}]']"
end
