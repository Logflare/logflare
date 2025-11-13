defmodule Logflare.NetworkingTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Networking

  describe "single tenant mode using Big Query" do
    TestUtils.setup_single_tenant()

    test "returns non empty list" do
      refute Networking.pools() == []
    end
  end

  describe "single tenant mode using Postgres" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "returns non empty list" do
      assert Networking.pools() == []
    end
  end
end
