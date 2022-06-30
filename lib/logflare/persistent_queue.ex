defmodule Logflare.PersistentQueue do
  @moduledoc """
  This is an implementation of a persistent queue GenStage producer.

  The goal is to only provide a persistence layer (leveraging existing libraries), but without worker management included.any()
  This allows for more flexibility when integrating in a data processing pipeline.
  """
end
