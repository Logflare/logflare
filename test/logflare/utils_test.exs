defmodule Logflare.UtilsTest do
  use ExUnit.Case, async: true

  doctest Logflare.EnumDeepUpdate, import: true
  doctest Logflare.Utils, import: true
  doctest Logflare.Utils.Map, import: true
end

defmodule Logflare.UtilsSyncTest do
  use ExUnit.Case, async: false

  describe "Tesla.Env stringification for test" do
    test "Tesla.Env stringification should error" do
      refute Logflare.Utils.stringify(%Tesla.Env{headers: [{"authorization", "some token"}]}) =~
               "REDACTED"

      refute Logflare.Utils.stringify(%Tesla.Env{headers: [{"x-api-key", "some token"}]}) =~
               "REDACTED"

      refute inspect(%Tesla.Env{headers: [{"authorization", "some token"}]}) =~ "REDACTED"
      refute inspect(%Tesla.Env{headers: [{"x-api-key", "some token"}]}) =~ "REDACTED"
    end
  end

  describe "Tesla.Env stringification for staging/prod" do
    setup do
      Application.put_env(:logflare, :env, :prod)

      on_exit(fn ->
        Application.put_env(:logflare, :env, :test)
      end)

      :ok
    end

    test "Tesla.Env stringification should error" do
      assert Logflare.Utils.stringify(%Tesla.Env{headers: [{"authorization", "some token"}]}) =~
               "REDACTED"

      assert Logflare.Utils.stringify(%Tesla.Env{headers: [{"x-api-key", "some token"}]}) =~
               "REDACTED"

      assert inspect(%Tesla.Env{headers: [{"authorization", "some token"}]}) =~ "REDACTED"
      assert inspect(%Tesla.Env{headers: [{"x-api-key", "some token"}]}) =~ "REDACTED"
    end
  end
end
