ExUnit.start()

defmodule BumpReleaseTest do
  use ExUnit.Case, async: false

  @chart """
  apiVersion: v2
  name: logflare
  # Chart version comment.
  version: 0.6.1 # keep this comment
  appVersion: "1.53.0"
  annotations:
    version: unrelated
  """

  setup do
    root = Path.join(System.tmp_dir!(), "bump-release-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "scripts"))
    File.mkdir_p!(Path.join(root, "helm"))
    script = Path.join(root, "scripts/bump_release.exs")
    File.cp!(Path.join(__DIR__, "bump_release.exs"), script)
    version = Path.join(root, "VERSION")
    chart = Path.join(root, "helm/Chart.yaml")
    File.write!(version, "1.53.0\n")
    File.write!(chart, @chart)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, script: script, version: version, chart: chart}
  end

  test "release bumps application and chart patch", context do
    assert {_, 0} = run_script(context, ["1.54.0"])
    assert File.read!(context.version) == "1.54.0\n"

    expected =
      @chart
      |> String.replace("version: 0.6.1", "version: 0.6.2")
      |> String.replace("1.53.0", "1.54.0")

    assert File.read!(context.chart) == expected
    assert {_, 0} = run_script(context, ["--check"])
  end

  test "explicit chart version", context do
    assert {_, 0} = run_script(context, ["1.54.0", "--chart-version", "0.7.0"])
    assert File.read!(context.chart) =~ "version: 0.7.0 # keep this comment"
  end

  test "chart-only default and explicit version", context do
    original_version = File.read!(context.version)

    for {args, expected} <- [{[], "0.6.2"}, {["--chart-version", "0.7.0"], "0.7.0"}] do
      assert {_, 0} = run_script(context, ["--chart-only" | args])
      assert File.read!(context.version) == original_version

      assert File.read!(context.chart) ==
               String.replace(@chart, "version: 0.6.1", "version: #{expected}")
    end
  end

  test "dry runs show diff without writing", context do
    for args <- [["1.54.0"], ["--chart-only", "--chart-version", "0.7.0"]] do
      before = contents(context)
      assert {output, 0} = run_script(context, args ++ ["--dry-run"])
      assert output =~ "--- a/helm/Chart.yaml"
      assert output =~ "--- a/VERSION" == (hd(args) != "--chart-only")
      assert output =~ "-version: 0.6.1"
      assert contents(context) == before
    end
  end

  test "check is read-only", context do
    before = contents(context)
    assert {output, 0} = run_script(context, ["--check"])
    assert output =~ "Chart appVersion matches VERSION: 1.53.0"
    assert contents(context) == before
  end

  test "drift rejects checks and bumps", context do
    File.write!(context.chart, String.replace(@chart, "1.53.0", "1.52.0"))

    for args <- [["--check"], ["1.54.0"], ["--chart-only"]] do
      assert assert_rejected(context, args) =~ "does not match VERSION"
    end
  end

  test "invalid versions are rejected before writing", context do
    for version <- [
          "v1.54.0",
          "1.54",
          "01.54.0",
          "1.054.0",
          "1.54.00",
          "1.54.0-rc.1",
          "1.54.0+build",
          "1.54.0\n",
          "1.54.0;echo bad",
          ""
        ] do
      assert_rejected(context, [version])
      assert_rejected(context, ["1.54.0", "--chart-version", version])
    end
  end

  test "versions must increase numerically", context do
    for version <- ["1.53.0", "1.52.99", "0.99.99"], do: assert_rejected(context, [version])

    for version <- ["0.6.1", "0.6.0", "0.5.99"] do
      assert_rejected(context, ["1.54.0", "--chart-version", version])
    end

    assert {_, 0} = run_script(context, ["1.100.0", "--chart-version", "0.10.0"])
  end

  test "repeated release is rejected", context do
    assert {_, 0} = run_script(context, ["1.54.0"])
    assert_rejected(context, ["1.54.0"])
  end

  test "invalid argument combinations", context do
    for args <- [
          [],
          ["--chart-version", "0.7.0"],
          ["1.54.0", "--chart-only"],
          ["--check", "1.54.0"],
          ["--check", "--chart-only"],
          ["--check", "--dry-run"],
          ["--check", "--chart-version", "0.7.0"],
          ["1.54.0", "--unknown"],
          ["--check", ""],
          ["--check", "--chart-version", ""],
          ["--chart-only", ""],
          ["1.54.0", "1.55.0"],
          ["1.54.0", "--chart-version"]
        ] do
      assert_rejected(context, args)
    end
  end

  test "missing or malformed chart fields", context do
    for text <- [
          String.replace(@chart, "version: 0.6.1", "version: nope"),
          @chart <> "version: 0.7.0\n",
          @chart <> "appVersion: nope\n",
          String.replace(@chart, "appVersion: \"1.53.0\"\n", ""),
          String.replace(@chart, "\"1.53.0\"", "\"1.53.0'")
        ] do
      File.write!(context.chart, text)
      assert_rejected(context, ["1.54.0"])
      assert_rejected(context, ["--check"])
    end
  end

  test "invalid current version", context do
    File.write!(context.version, "not-a-version\n")
    assert_rejected(context, ["1.54.0"])
    assert_rejected(context, ["--check"])
  end

  test "supported YAML quotes and whitespace", context do
    for quote <- ["", "'", "\""] do
      File.write!(context.version, "1.53.0\n")

      text =
        @chart
        |> String.replace("version: 0.6.1", "version:  #{quote}0.6.1#{quote}")
        |> String.replace(
          "appVersion: \"1.53.0\"",
          "appVersion: #{quote}1.53.0#{quote} # app comment"
        )

      File.write!(context.chart, text)
      assert {_, 0} = run_script(context, ["1.54.0"])
      assert File.read!(context.chart) =~ "version:  #{quote}0.6.2#{quote} # keep this comment"
      assert File.read!(context.chart) =~ "appVersion: \"1.54.0\" # app comment"
    end
  end

  test "missing file reports error", context do
    File.rm!(context.chart)
    assert {output, status} = run_script(context, ["1.54.0"])
    assert status != 0
    assert output =~ "bump-release:"
    assert File.read!(context.version) == "1.53.0\n"
  end

  test "direct executable invocation", context do
    assert {_, 0} =
             System.cmd(context.script, ["--check"], cd: context.root, stderr_to_stdout: true)
  end

  test "help is read-only", context do
    before = contents(context)
    assert {output, 0} = run_script(context, ["--help"])
    assert output =~ "Usage: elixir scripts/bump_release.exs"
    assert contents(context) == before
  end

  test "dry-run handles missing final newline", context do
    File.write!(context.version, "1.53.0")
    before = contents(context)
    assert {output, 0} = run_script(context, ["1.54.0", "--dry-run"])
    assert output =~ "-1.53.0\n\\ No newline at end of file\n+1.54.0\n"
    assert contents(context) == before
  end

  @spec run_script(map(), [String.t()]) :: {String.t(), non_neg_integer()}
  defp run_script(context, args) do
    System.cmd(System.find_executable("elixir"), [context.script | args],
      cd: Path.join(context.root, "helm"),
      stderr_to_stdout: true
    )
  end

  @spec contents(map()) :: {String.t(), String.t()}
  defp contents(context), do: {File.read!(context.version), File.read!(context.chart)}

  @spec assert_rejected(map(), [String.t()]) :: String.t()
  defp assert_rejected(context, args) do
    before = contents(context)
    {output, status} = run_script(context, args)
    assert status != 0, "Expected rejection for #{inspect(args)}: #{output}"
    assert output =~ "bump-release:"
    assert contents(context) == before
    output
  end
end
