defmodule Logflare.EctoTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Repo
  test "after connect set schema if schema is set" do
    Repo.checkout(fn conn ->
      Logflare.Ecto.AfterConnect.run(conn)
      assert "custom_schema" = Repo.query!("select schema_name")
    end)
  end

end
