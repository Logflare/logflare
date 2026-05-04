defmodule Logflare.JSON do
  @moduledoc false

  def decode(iodata, options \\ []) do
    Jason.decode(iodata, options)
  end

  def decode!(iodata, options \\ []) do
    Jason.decode!(iodata, options)
  end

  def encode(iodata, options \\ []) do
    Jason.encode(iodata, options)
  end

  def encode!(iodata, options \\ []) do
    Jason.encode!(iodata, options)
  end
end
