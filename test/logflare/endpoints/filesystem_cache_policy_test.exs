defmodule Logflare.Endpoints.FilesystemCachePolicyTest do
  use ExUnit.Case, async: true

  alias Logflare.Endpoints.ClickHouseSettings
  alias Logflare.Endpoints.FilesystemCachePolicy

  @setting "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @policy %{
    "start_param" => "from",
    "end_param" => "to",
    "timestamp_format" => "iso8601",
    "bypass_fill_above_seconds" => 86_400
  }

  test "disabled and short/equal windows leave settings unspecified" do
    assert {:ok, %{window_seconds: nil, settings: %{}}} =
             FilesystemCachePolicy.resolve(nil, %{})

    for finish <- ["2026-10-01T01:00:00Z", "2026-10-02T00:00:00Z"] do
      assert {:ok, %{settings: settings}} =
               FilesystemCachePolicy.resolve(@policy, %{
                 "from" => "2026-10-01T00:00:00Z",
                 "to" => finish
               })

      assert settings == %{}
    end
  end

  test "strictly greater windows select existing-only mode with microsecond precision" do
    assert {:ok, %{window_seconds: 86_400.000001, settings: %{@setting => 1}}} =
             FilesystemCachePolicy.resolve(@policy, %{
               "from" => "2026-10-01T00:00:00Z",
               "to" => "2026-10-02T00:00:00.000001Z"
             })
  end

  test "ISO offsets are normalized to the same instant" do
    assert {:ok, %{window_seconds: +0.0, settings: %{}}} =
             FilesystemCachePolicy.resolve(@policy, %{
               "from" => "2026-10-01T00:00:00Z",
               "to" => "2026-10-01T02:00:00+02:00"
             })
  end

  test "Unix seconds and milliseconds accept integers and complete integer strings" do
    for {format, start, finish} <- [
          {"unix_seconds", -1, "86400"},
          {"unix_milliseconds", "-1000", 86_400_000}
        ] do
      policy = Map.put(@policy, "timestamp_format", format)

      assert {:ok, %{window_seconds: 86_401.0, settings: %{@setting => 1}}} =
               FilesystemCachePolicy.resolve(policy, %{"from" => start, "to" => finish})
    end
  end

  test "missing, malformed, timezone-free and reversed inputs fail explicitly" do
    for params <- [
          %{},
          %{"from" => "2026-10-01T00:00:00Z"},
          %{"from" => "2026-10-01", "to" => "2026-10-03"},
          %{"from" => "2026-10-01T00:00:00", "to" => "2026-10-03T00:00:00Z"},
          %{"from" => "2026-10-03T00:00:00Z", "to" => "2026-10-01T00:00:00Z"},
          %{"from" => %{}, "to" => []}
        ] do
      assert {:error, message} = FilesystemCachePolicy.resolve(@policy, params)
      assert message =~ "valid start/end timestamps"
    end

    for value <- [1.5, "123junk", "999999999999999999999999999", true] do
      policy = Map.put(@policy, "timestamp_format", "unix_seconds")
      assert {:error, _} = FilesystemCachePolicy.resolve(policy, %{"from" => 0, "to" => value})
    end
  end

  test "normalizes form units and disables the policy without retaining form metadata" do
    form =
      @policy
      |> Map.delete("bypass_fill_above_seconds")
      |> Map.merge(%{"enabled" => "true", "window_threshold" => "24", "window_unit" => "hours"})

    assert {:ok, @policy} = FilesystemCachePolicy.normalize(form)
    assert {:ok, nil} = FilesystemCachePolicy.normalize(Map.put(form, "enabled", "false"))

    for changes <- [
          %{"start_param" => "to"},
          %{"end_param" => ""},
          %{"timestamp_format" => "guess"},
          %{"bypass_fill_above_seconds" => 0},
          %{"bypass_fill_above_seconds" => -1},
          %{"bypass_fill_above_seconds" => "1.5"},
          %{"window_threshold" => 1, "window_unit" => "unknown"},
          %{"arbitrary_setting" => 1}
        ] do
      assert {:error, _} = FilesystemCachePolicy.normalize(Map.merge(@policy, changes))
    end
  end

  test "trusted boolean values support 0/1 but not arbitrary literals" do
    for value <- [0, 1] do
      assert {:ok, sql} = ClickHouseSettings.enforce("SELECT 1", %{@setting => value})
      assert sql =~ "#{@setting} = #{value}"
    end

    for value <- [-1, 2, "1", true] do
      assert {:error, _} = ClickHouseSettings.normalize(%{@setting => value})
    end
  end
end
