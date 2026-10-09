defmodule Logflare.QA.Report do
  @moduledoc """
  Prints QA results as `QA_CHECK PASS|FAIL <label>` lines, shared by `mix qa.ingest`
  and `mix qa.ui`.
  """

  @spec check(String.t(), boolean(), String.t() | nil) :: boolean()
  def check(label, ok?, detail \\ nil) do
    suffix = if ok? or is_nil(detail), do: "", else: " (#{detail})"
    Mix.shell().info("QA_CHECK #{if ok?, do: "PASS", else: "FAIL"} #{label}#{suffix}")
    ok?
  end

  @doc "Runs a browser spec. A spec signals a failed assertion by raising."
  @spec spec(String.t(), (-> [Path.t()])) :: boolean()
  def spec(label, fun) do
    captures = fun.()
    Enum.each(captures, &Mix.shell().info("QA_CAPTURE #{&1}"))
    check(label, true)
  rescue
    error in [ArgumentError, MatchError, RuntimeError] ->
      check(label, false, Exception.message(error))
  end

  @spec finish!([boolean()], String.t()) :: :ok
  def finish!(results, label) do
    if Enum.all?(results) do
      Mix.shell().info("QA_RESULT PASS #{label}")
    else
      Mix.raise("QA_RESULT FAIL #{label}")
    end
  end
end
