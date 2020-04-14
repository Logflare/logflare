defmodule Logflare.JSON do
  @moduledoc false

  def decode(iodata, options \\ []) do
    Jsonrs.decode(iodata, options)
  end

  def decode!(iodata, options \\ []) do
    Jsonrs.decode!(iodata, options)
  end

  def encode(iodata, options \\ []) do
    Jsonrs.encode(iodata, options)
  end

  def encode!(iodata, options \\ []) do
    Jsonrs.encode!(iodata, options)
  end

end
