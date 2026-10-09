#!/usr/bin/env elixir

defmodule BumpRelease do
  @moduledoc """
  Prepares application or chart-only releases without publishing anything.
  """

  @usage """
  Usage: elixir scripts/bump_release.exs VERSION [options]
         elixir scripts/bump_release.exs --chart-only [options]
         elixir scripts/bump_release.exs --check

  Versions must be stable X.Y.Z with no leading zeros.

    --chart-only             Bump only the Helm chart version
    --chart-version VERSION  Explicit chart version; defaults to a patch bump
    --dry-run                Print the proposed diff without writing
    --check                  Validate current versions without writing
    --help                   Show this help
  """

  @spec main([String.t()]) :: :ok
  def main(argv) do
    {options, args} =
      OptionParser.parse!(argv,
        strict: [
          chart_only: :boolean,
          chart_version: :string,
          dry_run: :boolean,
          check: :boolean,
          help: :boolean
        ]
      )

    if options[:help] do
      IO.puts(@usage)
    else
      mode = release_mode!(options, args)
      prepare_release(mode, options)
    end
  end

  @spec release_mode!(keyword(), [String.t()]) :: :check | :chart_only | {:app, String.t()}
  defp release_mode!(options, args) do
    cond do
      options[:check] ->
        if args != [] or Keyword.delete(options, :check) != [] do
          raise ArgumentError, "--check cannot be combined with bump options"
        end

        :check

      options[:chart_only] ->
        if args != [] do
          raise ArgumentError, "--chart-only cannot be combined with an application version"
        end

        :chart_only

      match?([_], args) ->
        [version] = args
        {:app, version}

      true ->
        raise ArgumentError, "provide an application version or --chart-only\n\n#{@usage}"
    end
  end

  @spec prepare_release(:check | :chart_only | {:app, String.t()}, keyword()) :: :ok
  defp prepare_release(mode, options) do
    root = Path.expand("..", __DIR__)
    version_path = Path.join(root, "VERSION")
    chart_path = Path.join(root, "helm/Chart.yaml")
    version_text = File.read!(version_path)
    chart_text = File.read!(chart_path)
    current_app = String.trim(version_text)
    parse_version!(current_app)
    {_, _, chart_app, _} = chart_field!(chart_text, "appVersion")
    {_, _, current_chart, _} = chart_field!(chart_text, "version")
    chart_version = parse_version!(current_chart)

    if chart_app != current_app do
      raise ArgumentError,
            "helm/Chart.yaml appVersion (#{chart_app}) does not match VERSION (#{current_app}). " <>
              "Resolve the mismatch before bumping."
    end

    case mode do
      :check ->
        IO.puts(
          "Chart appVersion matches VERSION: #{current_app}; chart version: #{current_chart}"
        )

      _ ->
        new_app = application_version!(mode, current_app)
        default_chart = "#{chart_version.major}.#{chart_version.minor}.#{chart_version.patch + 1}"
        new_chart = Keyword.get(options, :chart_version, default_chart)
        ensure_increase!(new_chart, current_chart, "Chart")
        new_chart_text = replace_chart_field(chart_text, "version", new_chart)

        changes =
          case mode do
            :chart_only ->
              [{chart_path, chart_text, new_chart_text}]

            {:app, _} ->
              [
                {version_path, version_text, new_app <> "\n"},
                {chart_path, chart_text,
                 replace_chart_field(new_chart_text, "appVersion", new_app)}
              ]
          end

        Enum.each(changes, fn {path, old, new} ->
          if options[:dry_run] do
            print_diff(Path.relative_to(path, root), old, new)
          else
            File.write!(path, new)
          end
        end)

        unless options[:dry_run] do
          IO.puts(
            "Application: #{current_app} -> #{new_app}; chart: #{current_chart} -> #{new_chart}"
          )
        end
    end

    :ok
  end

  @spec application_version!(:chart_only | {:app, String.t()}, String.t()) :: String.t()
  defp application_version!(:chart_only, current), do: current

  defp application_version!({:app, version}, current) do
    ensure_increase!(version, current, "Application")
    version
  end

  @spec parse_version!(String.t()) :: Version.t()
  defp parse_version!(text) do
    with {:ok, %Version{pre: [], build: nil} = version} <- Version.parse(text),
         true <- to_string(version) == text do
      version
    else
      _ ->
        raise ArgumentError,
              "Invalid version #{inspect(text)}; expected stable X.Y.Z (no leading zeros)."
    end
  end

  @spec ensure_increase!(String.t(), String.t(), String.t()) :: :ok
  defp ensure_increase!(new, current, kind) do
    if Version.compare(parse_version!(new), parse_version!(current)) != :gt do
      raise ArgumentError, "#{kind} version must be greater than #{current}."
    end

    :ok
  end

  @spec chart_field!(String.t(), String.t()) :: {String.t(), String.t(), String.t(), String.t()}
  defp chart_field!(text, key) do
    case Regex.scan(~r/^#{key}:[^\n]*$/m, text) do
      [[line]] ->
        case Regex.run(
               ~r/^(#{key}:[ \t]*)(["']?)([0-9]+\.[0-9]+\.[0-9]+)\2([ \t]*(?:#.*)?)$/,
               line
             ) do
          [_, prefix, quote, version, suffix] ->
            parse_version!(version)
            {prefix, quote, version, suffix}

          _ ->
            raise ArgumentError,
                  "Unsupported #{key} field in helm/Chart.yaml; expected stable X.Y.Z."
        end

      _ ->
        raise ArgumentError, "helm/Chart.yaml must contain exactly one top-level #{key} field."
    end
  end

  @spec replace_chart_field(String.t(), String.t(), String.t()) :: String.t()
  defp replace_chart_field(text, key, value) do
    {prefix, current_quote, _, suffix} = chart_field!(text, key)
    quote = if key == "appVersion", do: "\"", else: current_quote

    Regex.replace(~r/^#{key}:[^\n]*$/m, text, fn _ ->
      prefix <> quote <> value <> quote <> suffix
    end)
  end

  @spec print_diff(String.t(), String.t(), String.t()) :: :ok
  defp print_diff(name, old, new) do
    old_lines = String.split(old, ~r/(?<=\n)/, trim: true)
    new_lines = String.split(new, ~r/(?<=\n)/, trim: true)
    IO.puts("--- a/#{name}\n+++ b/#{name}")
    IO.puts("@@ -1,#{length(old_lines)} +1,#{length(new_lines)} @@")

    Enum.each(List.myers_difference(old_lines, new_lines), fn {operation, lines} ->
      prefix =
        case operation do
          :eq -> " "
          :del -> "-"
          :ins -> "+"
        end

      Enum.each(lines, fn line ->
        IO.write(prefix <> line)
        unless String.ends_with?(line, "\n"), do: IO.write("\n\\ No newline at end of file\n")
      end)
    end)
  end
end

try do
  BumpRelease.main(System.argv())
rescue
  error in [ArgumentError, File.Error, OptionParser.ParseError] ->
    IO.puts(:stderr, "bump-release: #{Exception.message(error)}")
    System.halt(1)
end
