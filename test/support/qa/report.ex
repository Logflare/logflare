defmodule Logflare.QA.Report do
  @moduledoc "Prints `QA_CHECK`, `QA_CAPTURE` and `QA_RESULT` lines for the QA tasks."

  @spec check(String.t(), :ok | boolean() | {:error, term()}, String.t() | nil) :: boolean()
  def check(label, result, detail \\ nil)
  def check(label, :ok, _), do: check(label, true)
  def check(label, {:error, reason}, _), do: check(label, false, inspect(reason))

  def check(label, ok?, detail) do
    suffix = if ok? or is_nil(detail), do: "", else: " (#{detail})"
    Mix.shell().info("QA_CHECK #{if ok?, do: "PASS", else: "FAIL"} #{label}#{suffix}")
    ok?
  end

  @doc "Runs a browser spec, which signals a failed assertion by raising."
  @spec spec(String.t(), (-> [Path.t()])) :: boolean()
  def spec(label, fun) do
    Enum.each(fun.(), &Mix.shell().info("QA_CAPTURE #{&1}"))
    check(label, true)
  rescue
    error in [ArgumentError, MatchError, RuntimeError] ->
      check(label, false, Exception.message(error))
  end

  @spec finish!([boolean()], String.t()) :: :ok
  def finish!(results, label) do
    if Enum.all?(results),
      do: Mix.shell().info("QA_RESULT PASS #{label}"),
      else: Mix.raise("QA_RESULT FAIL #{label}")
  end
end
