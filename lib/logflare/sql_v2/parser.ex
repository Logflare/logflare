defmodule Logflare.SqlV2.Parser do
  defmodule Native do
    use Rustler, otp_app: :logflare, crate: "sqlparser_ex"

    # When your NIF is loaded, it will override this function.
    def parse(_dialect, _query), do: :erlang.nif_error(:nif_not_loaded)
    def to_string(_query), do: :erlang.nif_error(:nif_not_loaded)
  end

  def parse(dialect, query) when dialect in ["postgres", "bigquery"] do
    with {:ok, json} <- Native.parse(dialect, query) do
      Jason.decode(json)
    end
  end

  def to_string(ast) when is_map(ast), do: __MODULE__.to_string([ast])

  def to_string(asts) when is_list(asts) do
    asts
    |> Jason.encode!()
    |> Native.to_string()
  end
end
