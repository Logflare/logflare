defmodule E2e.Features.QueryClassSettingsTest do
  use Logflare.FeatureCase, async: false

  import Fluffy.Playwright, only: [focus: 2, new_page: 1, switch_page: 2]

  alias Fluffy.Event
  alias Logflare.Backends
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Playwright.Dialog

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
    session: session,
    backend: backend
  } do
    session
    |> visit(~p"/backends/#{backend.id}/edit")
    |> click(class_toggle("api_paid"))
    |> fill(policy_input("api_paid", "max_threads"), "4.0")
    |> click(save_button())
    |> assert(contain_text(by_css("#query-class-settings-error"), "require a default"))
    |> assert(attribute(class_toggle("api_paid"), "aria-expanded", "true"))
    |> assert(value(policy_input("api_paid", "max_threads"), "4.0"))

    assert Backends.get_backend(backend.id).config[:query_class_settings] in [nil, %{}]

    session
    |> fill(policy_input("default", "priority"), "5")
    |> fill(policy_input("default", "max_execution_time"), ".5")
    |> fill(policy_input("api_paid", "priority"), "10")
    |> click(save_button())
    |> assert(contain_text(by_role(:status), "Query-class settings saved"))

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
    |> assert(value(policy_input("default", "priority"), "5"))
    |> click(class_toggle("api_paid"))
    |> assert(value(policy_input("api_paid", "max_threads"), "4"))
  end

  test "backend editor resets a class to defaults and disables query-class policy", %{
    session: session,
    backend: backend,
    owner: owner
  } do
    assert {:ok, _backend} =
             Backends.configure_query_class_settings(owner, backend, %{
               "default" => %{"priority" => 5, "max_execution_time" => 2.5},
               "api_paid" => %{"priority" => 10, "max_threads" => 4, @cache_key => 1}
             })

    session
    |> visit(~p"/backends/#{backend.id}/edit")
    |> assert(value(policy_input("default", "priority"), "5"))
    |> click(class_toggle("api_paid"))
    |> assert(value(policy_input("api_paid", "max_threads"), "4"))
    |> assert(visible(by_css("#query-class-api_paid-cache-override")))
    |> fill(policy_input("default", "priority"), "0")
    |> fill(policy_input("api_paid", "max_threads"), "")
    |> press(policy_input("api_paid", "max_threads"), "-")
    |> assert(visible(invalid(policy_input("api_paid", "max_threads"))))
    |> click(by_role(class_section("api_paid"), :button, name: "Reset class to defaults"))
    |> assert(contain_text(by_role(:status), "Class reset in this draft"))
    |> assert(value(invalid(policy_input("default", "priority")), "0"))
    |> assert(visible(valid(policy_input("api_paid", "max_threads"))))
    |> refute(visible(by_css("#query-class-api_paid-cache-override")))

    assert Backends.get_backend(backend.id).config.query_class_settings["api_paid"][@cache_key] ==
             1

    session
    |> fill(policy_input("default", "priority"), "6")
    |> click(save_button())
    |> assert(contain_text(by_role(:status), "Query-class settings saved"))

    policy = Backends.get_backend(backend.id).config.query_class_settings
    refute Map.has_key?(policy, "api_paid")
    assert policy["default"]["priority"] == 6

    priority = policy_input("default", "priority")

    session
    |> fill(priority, "")
    |> press(priority, "-")
    |> assert(visible(invalid(priority)))
    |> once(Event.dialog(), &Dialog.accept!/1)
    |> click(by_role(:button, name: "Disable query-class policy"))
    |> assert(contain_text(by_role(:status), "Query-class policy disabled"))
    |> assert(visible(valid(priority)))

    assert Backends.get_backend(backend.id).config.query_class_settings == %{}
  end

  test "reconnecting discards a draft based on an older policy", %{
    session: session,
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

    session
    |> visit(path)
    |> assert(value(threads, "12"))
    |> fill(priority, "6")

    # Nothing in the UI drops the LiveView connection, so the test drives `liveSocket` itself.
    Fluffy.Playwright.evaluate(
      session,
      "new Promise(resolve => window.liveSocket.disconnect(() => resolve(true)))"
    )

    # Another tab saves a newer policy while the draft is offline.
    session
    |> switch_page(new_page(session))
    |> visit(path)
    |> fill(threads, "4")
    |> click(save_button())
    |> assert(contain_text(by_role(:status), "Query-class settings saved"))

    session
    |> focus(threads)
    |> assert(focused(threads))

    Fluffy.Playwright.evaluate(session, "window.liveSocket.connect()")

    session
    |> assert(value(threads, "4"))
    |> assert(value(priority, "5"))
    |> click(save_button())
    |> assert(contain_text(by_role(:status), "Query-class settings saved"))

    assert Backends.get_backend(backend.id).config.query_class_settings == %{
             "default" => %{"priority" => 5, "max_threads" => 4}
           }
  end

  test "saving reveals and focuses an invalid input in a collapsed class", %{
    session: session,
    backend: backend,
    owner: owner
  } do
    policy = %{"default" => %{"priority" => 5}, "api_paid" => %{"max_threads" => 4}}
    assert {:ok, _} = Backends.configure_query_class_settings(owner, backend, policy)

    session
    |> visit(~p"/backends/#{backend.id}/edit")
    |> click(class_toggle("api_paid"))
    |> fill(policy_input("api_paid", "max_threads"), "0")
    |> click(class_toggle("api_paid"))
    |> assert(attribute(class_toggle("api_paid"), "aria-expanded", "false"))
    |> click(save_button())
    |> assert(attribute(class_toggle("api_paid"), "aria-expanded", "true"))
    |> assert(focused(invalid(policy_input("api_paid", "max_threads"))))

    assert Backends.get_backend(backend.id).config.query_class_settings == policy
  end

  defp class_section(class), do: by_css("#query-class-#{class}")

  defp class_toggle(class), do: by_css("#query-class-#{class}-toggle")

  defp policy_input(class, key),
    do: by_css("input[name='query_class_settings[#{class}][#{key}]']")

  defp save_button, do: by_role(:button, name: "Save query-class settings")

  defp invalid(locator), do: and_(locator, by_css(":invalid"))

  defp valid(locator), do: and_(locator, by_css(":valid"))
end
