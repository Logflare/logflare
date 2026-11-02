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

  test "missing and null boundaries leave the cache hint unspecified" do
    for params <- [
          %{},
          %{"from" => "2026-10-01T00:00:00Z"},
          %{"to" => "2026-10-03T00:00:00Z"},
          %{"from" => nil, "to" => "2026-10-03T00:00:00Z"},
          %{"from" => "2026-10-01T00:00:00Z", "to" => nil},
          %{"from" => nil, "to" => nil}
        ] do
      assert {:ok, %{window_seconds: nil, settings: %{}}} =
               FilesystemCachePolicy.resolve(@policy, params)
    end
  end

  test "empty-string boundaries leave the cache hint unspecified in every format" do
    for {format, value} <- [
          {"iso8601", "2026-10-01T00:00:00Z"},
          {"unix_seconds", "7200"},
          {"unix_milliseconds", "7200000"}
        ],
        params <- [
          %{"from" => "", "to" => value},
          %{"from" => value, "to" => ""},
          %{"from" => "", "to" => ""},
          %{"from" => "", "to" => nil}
        ] do
      policy = Map.put(@policy, "timestamp_format", format)

      assert {:ok, %{window_seconds: nil, settings: %{}}} =
               FilesystemCachePolicy.resolve(policy, params)

      assert {:error, {:invalid_filesystem_cache_window, _}} =
               FilesystemCachePolicy.resolve(policy, %{"from" => "", "to" => "bad"})
    end
  end

  test "malformed, timezone-free and reversed inputs fail explicitly" do
    for params <- [
          %{"from" => "bad"},
          %{"to" => "bad"},
          %{"from" => " ", "to" => nil},
          %{"from" => "2026-10-01", "to" => "2026-10-03"},
          %{"from" => "2026-10-01T00:00:00", "to" => "2026-10-03T00:00:00Z"},
          %{"from" => "2026-10-03T00:00:00Z", "to" => "2026-10-01T00:00:00Z"},
          %{"from" => %{}, "to" => []}
        ] do
      assert {:error, {:invalid_filesystem_cache_window, message}} =
               FilesystemCachePolicy.resolve(@policy, params)

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

  test "invalid policies retain their configuration error" do
    policy = Map.put(@policy, "bypass_fill_above_seconds", 0)
    assert {:error, expected} = FilesystemCachePolicy.normalize(policy)

    assert {:error, {:invalid_filesystem_cache_policy, message}} =
             FilesystemCachePolicy.resolve(policy, %{})

    assert message == "Filesystem cache policy " <> expected
  end

  test "leaving ClickHouse clears a saved policy" do
    for language <- [:bq_sql, :pg_sql] do
      changeset =
        %Logflare.Endpoints.EndpointQuery{language: :ch_sql, filesystem_cache_policy: @policy}
        |> Ecto.Changeset.change(language: language)
        |> FilesystemCachePolicy.validate()

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :filesystem_cache_policy) == nil
    end
  end

  test "newly enabled forms receive defaults without changing saved or edited thresholds" do
    assert %{
             "timestamp_format" => "iso8601",
             "window_threshold" => 24,
             "window_unit" => "hours",
             "enabled" => "true"
           } =
             FilesystemCachePolicy.form_values(%{"enabled" => "true"})

    assert %{"window_threshold" => 86_400, "window_unit" => "seconds"} =
             FilesystemCachePolicy.form_values(@policy)

    edited = Map.merge(@policy, %{"window_threshold" => "", "window_unit" => "days"})

    assert %{"window_threshold" => "", "window_unit" => "days"} =
             FilesystemCachePolicy.form_values(edited)

    assert %{"enabled" => "false"} = FilesystemCachePolicy.form_values(%{"enabled" => "false"})
  end

  test "unchanged normalized mappings do not reparse SQL on unrelated updates" do
    data = %Logflare.Endpoints.EndpointQuery{
      language: :ch_sql,
      query: "SELECT (",
      filesystem_cache_policy: @policy
    }

    form =
      @policy
      |> Map.delete("bypass_fill_above_seconds")
      |> Map.merge(%{"enabled" => "true", "window_threshold" => "24", "window_unit" => "hours"})

    changeset =
      data
      |> Ecto.Changeset.cast(%{"description" => "edited", "filesystem_cache_policy" => form}, [
        :description,
        :filesystem_cache_policy
      ])
      |> FilesystemCachePolicy.validate()

    assert changeset.valid?
    assert Ecto.Changeset.get_change(changeset, :filesystem_cache_policy) == nil
    assert Ecto.Changeset.get_field(changeset, :filesystem_cache_policy) == @policy

    for attrs <- [
          %{query: "SELECT (again"},
          %{filesystem_cache_policy: Map.put(@policy, "start_param", "other")},
          %{language: :ch_sql}
        ] do
      original = if Map.has_key?(attrs, :language), do: %{data | language: :bq_sql}, else: data
      changed = original |> Ecto.Changeset.change(attrs) |> FilesystemCachePolicy.validate()
      refute changed.valid?
      assert changed.errors[:filesystem_cache_policy]
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
