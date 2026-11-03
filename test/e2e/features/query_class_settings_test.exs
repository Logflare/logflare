defmodule E2e.Features.QueryClassSettingsTest do
  use Logflare.FeatureCase, async: false

  alias Logflare.Backends
  alias Logflare.Repo
  alias Logflare.SingleTenant

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

    %{backend: backend}
  end

  @tag accept_dialogs: true
  test "backend editor validates, saves, reloads and disables query-class policy", %{
    conn: conn,
    backend: backend
  } do
    session =
      conn
      |> visit(~p"/backends/#{backend.id}/edit")
      |> assert_has("#query-class-settings-form")
      |> click("#query-class-api_paid-toggle")
      |> fill_in("#query-class-api_paid_max_threads", "Max threads", with: "4")
      |> click_button("Save query-class settings")
      |> assert_has("#query-class-settings-error", text: "require a default")
      |> assert_has("#query-class-api_paid-toggle[aria-expanded='true']")
      |> assert_has("#query-class-api_paid_max_threads[value='4']")
      |> screenshot("query-class-01-validation.png")

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
      |> screenshot("query-class-02-saved.png")

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
      |> assert_has("#query-class-default_priority[value='5']")
      |> click("#query-class-api_paid-toggle")
      |> assert_has("#query-class-api_paid_max_threads[value='4']")
      |> screenshot("query-class-03-reloaded.png")
      |> fill_in("#query-class-api_paid_priority", "Priority", with: "")
      |> fill_in("#query-class-api_paid_max_threads", "Max threads", with: "")
      |> click_button("Save query-class settings")
      |> assert_has("[role=status]", text: "Query-class settings saved")
      |> screenshot("query-class-04-inherited.png")

    refute Map.has_key?(Backends.get_backend(backend.id).config.query_class_settings, "api_paid")

    session
    |> click_button("Disable query-class policy")
    |> assert_has("[role=status]", text: "Query-class policy disabled")
    |> screenshot("query-class-05-disabled.png")

    assert Backends.get_backend(backend.id).config.query_class_settings == %{}
  end
end
