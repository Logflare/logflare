defmodule Logflare.SourcesTest do
  @moduledoc false
  alias Logflare.Sources.Cache
  import Cache
  use Logflare.DataCase
  import Logflare.DummyFactory
  alias Logflare.{Repo, User, Source}

  setup do
    source = insert(:source, token: Faker.UUID.v4())
    _s2 = insert(:source, token: Faker.UUID.v4())
    {:ok, source: source}
  end

  describe "source cache" do
    test "get_by_id/1", %{source: source} do
      assert get_by_id(source.token) == source
    end
  end
end
