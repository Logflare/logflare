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

    test "creates an ingest token for sources selected from the combobox", %{conn: conn} do
      user = SingleTenant.get_default_user()
      source_name = "Combobox Source #{System.unique_integer([:positive])}"
      other_source_name = "Other Source #{System.unique_integer([:positive])}"
      description = "combobox access token #{System.unique_integer([:positive])}"
      source = insert(:source, user: user, name: source_name)
      other_source = insert(:source, user: user, name: other_source_name)
      source_option = source_name
      other_source_option = other_source_name

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/access-tokens")
      |> assert_has("[data-phx-main].phx-connected")
      |> click_button("Create access token")
      |> assert_path(~p"/access-tokens/new")
      |> choose("Selected sources")
      |> click_button("Open options")
      |> fill_in("input[role='combobox']", "Select sources...",
        with: String.slice(source_name, 0, 2)
      )
      |> assert_has("[role='option']", text: source_option)
      |> refute_has("[role='option']", text: other_source_option)
      |> click("[role='option']", source_option)
      |> fill_in("input[role='combobox']", "Select sources...",
        with: String.slice(other_source_name, 0, 2)
      )
      |> click("[role='option']", other_source_option)
      |> assert_has("button[aria-label='Remove #{source_option}']")
      |> assert_has("button[aria-label='Remove #{other_source_option}']")
      |> check("Private", exact: false)
      |> assert_has("input:checked:disabled", label: "All sources")
      |> refute_has("button[aria-label='Remove #{source_option}']")
      |> uncheck("Private", exact: false)
      |> assert_has("input:checked", label: "Selected sources")
      |> assert_has("button[aria-label='Remove #{source_option}']")
      |> assert_has("button[aria-label='Remove #{other_source_option}']")
      |> fill_in("Description", with: description)
      |> click_button("Create")
      |> assert_path(~p"/access-tokens")
      |> assert_has("*", text: "Access token created successfully")

      token = Enum.find(Auth.list_valid_access_tokens(user), &(&1.description == description))

      assert MapSet.new(String.split(token.scopes)) ==
               MapSet.new(["ingest:source:#{source.id}", "ingest:source:#{other_source.id}"])
    end

    test "restores disabled source selections after failed validation", %{conn: conn} do
      user = SingleTenant.get_default_user()
      source = insert(:source, user: user, name: "Validation recovery source")

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> visit(~p"/access-tokens/new")
      |> assert_has("[data-phx-main].phx-connected")
      |> choose("Selected sources")
      |> fill_in("input[role='combobox']", "Select sources...", with: source.name)
      |> click("[role='option']", source.name)
      |> uncheck("Ingest", exact: false)
      |> click_button("Create")
      |> assert_has("*", text: "select at least one scope")
      |> check("Ingest", exact: false)
      |> assert_has("input:checked", label: "Selected sources")
      |> assert_has("button[aria-label='Remove #{source.name}']")
    end

    test "does not submit an unrestricted token when Enter has no highlighted source", %{
      conn: conn
    } do
      user = SingleTenant.get_default_user()
      description = "unsubmitted combobox token #{System.unique_integer([:positive])}"

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/access-tokens")
      |> assert_has("[data-phx-main].phx-connected")
      |> click_button("Create access token")
      |> assert_path(~p"/access-tokens/new")
      |> fill_in("Description", with: description)
      |> choose("Selected sources")
      |> click_button("Open options")
      |> fill_in("input[role='combobox']", "Select sources...", with: "no matching source")
      |> press("[role='combobox'][aria-label='Select sources...']", "Enter")
      |> assert_has("input", label: "Description", value: description)

      refute Enum.any?(Auth.list_valid_access_tokens(user), &(&1.description == description))
    end
  end
end
