defmodule E2e.Features.QueryClassSettingsTest do
  use ExUnit.Case, async: false

  setup :configure_recording

  use Logflare.FeatureCase, async: false

  alias Logflare.Backends
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias PlaywrightEx.BrowserContext
  alias PlaywrightEx.Frame

  @viewport %{width: 1280, height: 1900}
  @cache_key "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"

  @spec configure_recording(map()) :: keyword()
  defp configure_recording(%{recording: recording}) do
    directory = Path.join(["screenshots", "query-class-videos", recording, Ecto.UUID.generate()])

    [
      recording_dir: directory,
      browser_context_opts: [
        viewport: @viewport,
        record_video: %{dir: directory, size: @viewport}
      ]
    ]
  end

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

  @tag recording: "save"
  test "backend editor validates, saves and reloads query-class policy", %{
    conn: conn,
    backend: backend,
    recording_dir: directory
  } do
    session =
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has(".phx-connected #query-class-settings-form")
      |> focus_editor()
      |> click("#query-class-api_paid-toggle")
      |> fill_in("#query-class-api_paid_max_threads", "Max threads", with: "4")
      |> click_button("Save query-class settings")
      |> assert_has("#query-class-settings-error", text: "require a default")
      |> assert_has("#query-class-api_paid-toggle[aria-expanded='true']")
      |> assert_has("#query-class-api_paid_max_threads[value='4']")
      |> focus_editor()
      |> screenshot("query-class-01-validation.png", full_page: false)

    assert Backends.get_backend(backend.id).config[:query_class_settings] in [nil, %{}]

    session =
      session
      |> fill_in("#query-class-default_priority", "Priority", with: "5")
      |> fill_in("#query-class-default_max_execution_time", "Max execution time (seconds)",
        with: "2.5"
      )
      |> fill_in("#query-class-api_paid_priority", "Priority", with: "10")
      |> click_button("Save query-class settings")
      |> assert_has("[role=status]", text: "Query-class settings saved")
      |> focus_editor()
      |> screenshot("query-class-02-saved.png", full_page: false)

    assert Backends.get_backend(backend.id).config.query_class_settings == %{
             "default" => %{
               "priority" => 5,
               "max_execution_time" => 2.5,
               "timeout_overflow_mode" => "throw"
             },
             "api_paid" => %{"priority" => 10, "max_threads" => 4}
           }

    session =
      session
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has(".phx-connected #query-class-default_priority[value='5']")
      |> focus_editor()
      |> click("#query-class-api_paid-toggle")
      |> assert_has("#query-class-api_paid_max_threads[value='4']")
      |> focus_editor()
      |> screenshot("query-class-03-reloaded.png", full_page: false)

    finish_recording(session, directory)
  end

  @tag accept_dialogs: true, recording: "inheritance"
  test "backend editor resets a class to defaults and disables query-class policy", %{
    conn: conn,
    backend: backend,
    owner: owner,
    recording_dir: directory
  } do
    assert {:ok, _backend} =
             Backends.configure_query_class_settings(owner, backend, %{
               "default" => %{"priority" => 5, "max_execution_time" => 2.5},
               "api_paid" => %{"priority" => 10, "max_threads" => 4, @cache_key => 1}
             })

    session =
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has(".phx-connected #query-class-default_priority[value='5']")
      |> focus_editor()
      |> click("#query-class-api_paid-toggle")
      |> assert_has("#query-class-api_paid_max_threads[value='4']")
      |> assert_has("#query-class-api_paid-cache-override")
      |> fill_in("#query-class-default_priority", "Priority", with: "6")
      |> click_button("#query-class-api_paid button[name=reset_class]", "Reset class to defaults")
      |> assert_has("[role=status]", text: "Class reset in this draft")
      |> assert_has("#query-class-default_priority[value='6']")
      |> refute_has("#query-class-api_paid-cache-override")

    assert Backends.get_backend(backend.id).config.query_class_settings["api_paid"][@cache_key] ==
             1

    session =
      session
      |> click_button("Save query-class settings")
      |> assert_has("[role=status]", text: "Query-class settings saved")
      |> focus_editor()
      |> screenshot("query-class-04-inherited.png", full_page: false)

    policy = Backends.get_backend(backend.id).config.query_class_settings
    refute Map.has_key?(policy, "api_paid")
    assert policy["default"]["priority"] == 6

    session =
      session
      |> click_button("Disable query-class policy")
      |> assert_has("[role=status]", text: "Query-class policy disabled")
      |> focus_editor()
      |> screenshot("query-class-05-disabled.png", full_page: false)

    assert Backends.get_backend(backend.id).config.query_class_settings == %{}
    finish_recording(session, directory)
  end

  @spec focus_editor(PhoenixTest.Playwright.t()) :: PhoenixTest.Playwright.t()
  defp focus_editor(session) do
    unwrap(session, fn %{frame_id: frame_id} ->
      assert {:ok, _} =
               Frame.evaluate(frame_id,
                 expression: """
                 (() => {
                   const panel = document.querySelector('[id^="backend-query-class-settings-"]');
                   window.scrollTo({ top: panel.getBoundingClientRect().top + window.scrollY - 180, behavior: 'instant' });
                   return true;
                 })()
                 """,
                 timeout: 5_000
               )
    end)
  end

  @spec finish_recording(PhoenixTest.Playwright.t(), String.t()) :: :ok
  defp finish_recording(session, directory) do
    unwrap(session, fn %{context_id: context_id} ->
      assert {:ok, _} = BrowserContext.close(context_id, timeout: 15_000)
    end)

    assert [video] = Path.wildcard(Path.join(directory, "*.webm"))
    assert File.stat!(video).size > 0
    :ok
  end
end
