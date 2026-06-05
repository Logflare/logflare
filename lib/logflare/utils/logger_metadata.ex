defmodule Logflare.Utils.LoggerMetadata do
  @moduledoc false

  @spec with_metadata(Keyword.t(), (-> term())) :: term()
  def with_metadata(metadata, fun) when is_list(metadata) and is_function(fun, 0) do
    previous_metadata = Logger.metadata()

    Logger.metadata(metadata)

    try do
      fun.()
    after
      Logger.reset_metadata(previous_metadata)
    end
  end
end
