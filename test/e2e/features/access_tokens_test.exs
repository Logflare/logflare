defmodule E2e.Features.AccessTokensTest do
  use Logflare.FeatureCase, async: false

  alias Logflare.Auth
  alias Logflare.SingleTenant

  setup do
    start_supervised!(Logflare.SystemMetricsSup)

    :ok
  end

  describe "access token creation" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    test "creates an ingest token for sources selected from the combobox", %{session: session} do
      user = SingleTenant.get_default_user()
      source_name = "Combobox Source #{System.unique_integer([:positive])}"
      other_source_name = "Other Source #{System.unique_integer([:positive])}"
      description = "combobox access token #{System.unique_integer([:positive])}"
      source = insert(:source, user: user, name: source_name)
      other_source = insert(:source, user: user, name: other_source_name)

      session
      |> log_in()
      |> visit(~p"/access-tokens")
      |> click(by_role(:button, name: "Create access token"))
      |> assert(page_url(~p"/access-tokens/new"))
      |> check(by_label("Selected sources", exact: true))
      |> click(by_role(:button, name: "Open options"))
      |> fill(sources_combobox(), String.slice(source_name, 0, 2))
      |> assert(visible(by_role(:option, name: source_name)))
      |> refute(visible(by_role(:option, name: other_source_name)))
      |> click(by_role(:option, name: source_name))
      |> fill(sources_combobox(), String.slice(other_source_name, 0, 2))
      |> click(by_role(:option, name: other_source_name))
      |> assert(visible(remove_source_button(source_name)))
      |> assert(visible(remove_source_button(other_source_name)))
      |> check(by_label("Private"))
      |> assert(checked(by_label("All sources", exact: true)))
      |> assert(disabled(by_label("All sources", exact: true)))
      |> refute(visible(remove_source_button(source_name)))
      |> uncheck(by_label("Private"))
      |> assert(checked(by_label("Selected sources", exact: true)))
      |> assert(visible(remove_source_button(source_name)))
      |> assert(visible(remove_source_button(other_source_name)))
      |> fill(by_label("Description"), description)
      |> click(by_role(:button, name: "Create", exact: true))
      |> assert(page_url(~p"/access-tokens"))
      |> assert(visible(by_text("Access token created successfully")))

      token = Enum.find(Auth.list_valid_access_tokens(user), &(&1.description == description))

      assert MapSet.new(String.split(token.scopes)) ==
               MapSet.new(["ingest:source:#{source.id}", "ingest:source:#{other_source.id}"])
    end

    test "restores disabled source selections after failed validation", %{session: session} do
      user = SingleTenant.get_default_user()
      source = insert(:source, user: user, name: "Validation recovery source")

      session
      |> log_in()
      |> visit(~p"/access-tokens/new")
      |> check(by_label("Selected sources", exact: true))
      |> fill(sources_combobox(), source.name)
      |> click(by_role(:option, name: source.name))
      |> uncheck(by_label("Ingest"))
      |> click(by_role(:button, name: "Create", exact: true))
      |> assert(visible(by_text("select at least one scope")))
      |> check(by_label("Ingest"))
      |> assert(checked(by_label("Selected sources", exact: true)))
      |> assert(visible(remove_source_button(source.name)))
    end

    test "does not submit an unrestricted token when Enter has no highlighted source", %{
      session: session
    } do
      user = SingleTenant.get_default_user()
      description = "unsubmitted combobox token #{System.unique_integer([:positive])}"

      session
      |> log_in()
      |> visit(~p"/access-tokens")
      |> click(by_role(:button, name: "Create access token"))
      |> assert(page_url(~p"/access-tokens/new"))
      |> fill(by_label("Description"), description)
      |> check(by_label("Selected sources", exact: true))
      |> click(by_role(:button, name: "Open options"))
      |> fill(sources_combobox(), "no matching source")
      |> press(sources_combobox(), "Enter")
      |> assert(value(by_label("Description"), description))

      refute Enum.any?(Auth.list_valid_access_tokens(user), &(&1.description == description))
    end
  end

  defp sources_combobox, do: by_role(:combobox, name: "Select sources...")

  defp remove_source_button(source_name), do: by_role(:button, name: "Remove #{source_name}")
end
