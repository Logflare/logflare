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

    test "creates an ingest token for a source selected from the combobox", %{conn: conn} do
      user = SingleTenant.get_default_user()
      source_name = "Combobox Source #{System.unique_integer([:positive])}"
      other_source_name = "Other Source #{System.unique_integer([:positive])}"
      description = "combobox access token #{System.unique_integer([:positive])}"
      source = insert(:source, user: user, name: source_name)
      insert(:source, user: user, name: other_source_name)
      source_option = "Ingest into #{source_name} only"
      other_source_option = "Ingest into #{other_source_name} only"

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/access-tokens")
      |> assert_has("[data-phx-main].phx-connected")
      |> click_button("Create access token")
      |> click("#scopes-ingest-0-combobox button")
      |> type("#scopes-ingest-0-combobox input[role='combobox']", String.slice(source_name, 0, 2))
      |> assert_has("[role='option']", text: source_option)
      |> refute_has("[role='option']", text: other_source_option)
      |> click("[role='option']", source_option)
      |> type("input[name='description']", description)
      |> click_button("button[type='submit']", "Create")
      |> assert_has("*", text: "Access token created successfully")

      token = Enum.find(Auth.list_valid_access_tokens(user), &(&1.description == description))

      assert token.scopes == "ingest:source:#{source.id}"
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
      |> type("input[name='description']", description)
      |> click("#scopes-ingest-0-combobox button")
      |> type("#scopes-ingest-0-combobox input[role='combobox']", "no matching source")
      |> press("#scopes-ingest-0-combobox input[role='combobox']", "Enter")
      |> assert_has("input[name='description']")

      refute Enum.any?(Auth.list_valid_access_tokens(user), &(&1.description == description))
    end
  end
end
