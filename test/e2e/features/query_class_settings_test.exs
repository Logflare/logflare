defmodule E2e.Features.QueryClassSettingsTest do
  use Logflare.FeatureCase, async: false

  alias Logflare.Backends
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias PhoenixTest.Playwright
  alias PlaywrightEx.BrowserContext
  alias PlaywrightEx.Frame

  @cache_key "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"

  setup do
    start_supervised!(Logflare.SystemMetricsSup)
    :ok
  end

  TestUtils.setup_single_tenant(seed_user: true)

  setup do
    owner =
      SingleTenant.get_default_user() |> Ecto.Changeset.change(admin: false) |> Repo.update!()

    backend =
      insert(:backend,
        user: owner,
        type: :clickhouse,
        config: %{url: "https://ingest.example.com", port: 8443, database: "default"}
      )

    %{backend: backend, owner: owner}
  end

  test "backend editor validates, saves and reloads query-class policy", %{
    conn: conn,
    backend: backend
  } do
    session =
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has(".phx-connected #query-class-settings-form")
      |> click("#query-class-api_paid-toggle")
      |> fill_in(policy_input("api_paid", "max_threads"), "Max threads", with: "4")
      |> click_button("Save query-class settings")
      |> assert_has("#query-class-settings-error", text: "require a default")
      |> assert_has("#query-class-api_paid-toggle[aria-expanded='true']")
      |> assert_has("#{policy_input("api_paid", "max_threads")}[value='4']")

    assert Backends.get_backend(backend.id).config[:query_class_settings] in [nil, %{}]

    session =
      session
      |> fill_in(policy_input("default", "priority"), "Priority", with: "5")
      |> fill_in(policy_input("default", "max_execution_time"), "Max execution time (seconds)",
        with: ".5"
      )
      |> fill_in(policy_input("api_paid", "priority"), "Priority", with: "10")
      |> click_button("Save query-class settings")
      |> assert_has("[role=status]", text: "Query-class settings saved")

    assert Backends.get_backend(backend.id).config.query_class_settings == %{
             "default" => %{
               "priority" => 5,
               "max_execution_time" => 0.5,
               "timeout_overflow_mode" => "throw"
             },
             "api_paid" => %{"priority" => 10, "max_threads" => 4}
           }

    session
    |> visit(~p"/backends/#{backend.id}/edit")
    |> assert_has(".phx-connected #{policy_input("default", "priority")}[value='5']")
    |> click("#query-class-api_paid-toggle")
    |> assert_has("#{policy_input("api_paid", "max_threads")}[value='4']")
  end

  @tag accept_dialogs: true
  test "backend editor resets a class to defaults and disables query-class policy", %{
    conn: conn,
    backend: backend,
    owner: owner
  } do
    assert {:ok, _backend} =
             Backends.configure_query_class_settings(owner, backend, %{
               "default" => %{"priority" => 5, "max_execution_time" => 2.5},
               "api_paid" => %{"priority" => 10, "max_threads" => 4, @cache_key => 1}
             })

    session =
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has(".phx-connected #{policy_input("default", "priority")}[value='5']")
      |> click("#query-class-api_paid-toggle")
      |> assert_has("#{policy_input("api_paid", "max_threads")}[value='4']")
      |> assert_has("#query-class-api_paid-cache-override")
      |> fill_in(policy_input("default", "priority"), "Priority", with: "6")
      |> fill_in(policy_input("api_paid", "max_threads"), "Max threads", with: "")
      |> Playwright.press(policy_input("api_paid", "max_threads"), "-")
      |> assert_has("#{policy_input("api_paid", "max_threads")}:invalid")
      |> click_button("#query-class-api_paid button[name=reset_class]", "Reset class to defaults")
      |> assert_has("[role=status]", text: "Class reset in this draft")
      |> assert_has("#{policy_input("default", "priority")}[value='6']")
      |> assert_has("#{policy_input("api_paid", "max_threads")}:valid")
      |> refute_has("#query-class-api_paid-cache-override")

    assert Backends.get_backend(backend.id).config.query_class_settings["api_paid"][@cache_key] ==
             1

    session =
      session
      |> click_button("Save query-class settings")
      |> assert_has("[role=status]", text: "Query-class settings saved")

    policy = Backends.get_backend(backend.id).config.query_class_settings
    refute Map.has_key?(policy, "api_paid")
    assert policy["default"]["priority"] == 6

    priority = policy_input("default", "priority")

    session
    |> fill_in(priority, "Priority", with: "")
    |> Playwright.press(priority, "-")
    |> assert_has("#{priority}:invalid")
    |> click_button("Disable query-class policy")
    |> assert_has("[role=status]", text: "Query-class policy disabled")
    |> assert_has("#{priority}:valid")

    assert Backends.get_backend(backend.id).config.query_class_settings == %{}
  end

  test "reconnecting discards a draft based on an older policy", %{
    conn: conn,
    backend: backend,
    owner: owner
  } do
    path = ~p"/backends/#{backend.id}/edit"
    priority = policy_input("default", "priority")
    threads = policy_input("default", "max_threads")

    assert {:ok, _} =
             Backends.configure_query_class_settings(owner, backend, %{
               "default" => %{"priority" => 5, "max_threads" => 12}
             })

    draft =
      conn
      |> visit(path)
      |> assert_has(".phx-connected #{threads}[value='12']")
      |> fill_in(priority, "Priority", with: "6")
      |> evaluate("new Promise(resolve => window.liveSocket.disconnect(() => resolve(true)))")

    unwrap(draft, fn %{context_id: context_id} ->
      assert {:ok, page} = BrowserContext.new_page(context_id, timeout: 5_000)

      Playwright.build(%{
        context_id: context_id,
        page_id: page.guid,
        frame_id: page.main_frame.guid,
        tracing_id: nil,
        config: []
      })
      |> visit(path)
      |> assert_has(".phx-connected #query-class-settings-form")
      |> fill_in(threads, "Max threads", with: "4")
      |> click_button("Save query-class settings")
      |> assert_has("[role=status]", text: "Query-class settings saved")
    end)

    draft
    |> evaluate("document.querySelector(#{Jason.encode!(threads)}).focus()")
    |> assert_has("#{threads}:focus")
    |> evaluate("window.liveSocket.connect()")
    |> assert_has(".phx-connected #{threads}[value='4']")
    |> assert_has("#{priority}[value='5']")
    |> click_button("Save query-class settings")
    |> assert_has("[role=status]", text: "Query-class settings saved")

    assert Backends.get_backend(backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5, "max_threads" => 4}
           }
  end

  test "saving reveals and focuses an invalid input in a collapsed class", %{
    conn: conn,
    backend: backend,
    owner: owner
  } do
    policy = %{"default" => %{"priority" => 5}, "api_paid" => %{"max_threads" => 4}}
    assert {:ok, _} = Backends.configure_query_class_settings(owner, backend, policy)

    conn
    |> visit(~p"/backends/#{backend.id}/edit")
    |> assert_has(".phx-connected #query-class-settings-form")
    |> click("#query-class-api_paid-toggle")
    |> fill_in(policy_input("api_paid", "max_threads"), "Max threads", with: "0")
    |> click("#query-class-api_paid-toggle")
    |> assert_has("#query-class-api_paid-toggle[aria-expanded='false']")
    |> click_button("Save query-class settings")
    |> assert_has("#query-class-api_paid-toggle[aria-expanded='true']", timeout: 2_000)
    |> assert_has("#{policy_input("api_paid", "max_threads")}:invalid:focus")

    assert Backends.get_backend(backend.id).config.query_class_settings == policy
  end

  @spec policy_input(String.t(), String.t()) :: String.t()
  defp policy_input(class, key), do: "input[name='query_class_settings[#{class}][#{key}]']"

  @spec evaluate(PhoenixTest.Playwright.t(), String.t()) :: PhoenixTest.Playwright.t()
  defp evaluate(session, expression) do
    unwrap(session, fn %{frame_id: frame_id} ->
      assert {:ok, _} = Frame.evaluate(frame_id, expression: expression, timeout: 5_000)
    end)
  end
end
