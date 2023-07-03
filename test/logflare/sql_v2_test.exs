defmodule Logflare.SqlV2Test do
  use Logflare.DataCase

  alias Logflare.TestUtils
  alias Logflare.SqlV2
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo

  describe "transform/3 for :postgres backends" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, name: "source_#{TestUtils.random_string()}")
      %{user: user, source: source}
    end

    test "changes query on FROM command to correct table name", %{
      source: %{name: name} = source,
      user: user
    } do
      input = "SELECT body, event_message, timestamp FROM #{name}"
      expected = {:ok, "SELECT body, event_message, timestamp FROM #{Repo.table_name(source)}"}
      assert SqlV2.transform(:postgres, input, user) == expected
    end
  end
end
