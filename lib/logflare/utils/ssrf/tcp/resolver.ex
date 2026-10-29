defmodule Logflare.Utils.SSRF.TCP.Resolver do
  @moduledoc false

  @type address :: String.t() | :inet.hostname() | :inet.ip_address()
  @type family :: :inet | :inet6
  @type timer :: false | reference()

  @spec getaddrs(address(), family(), timer()) ::
          {:ok, [:inet.ip_address()]} | {:error, atom()}
  def getaddrs(address, :inet, timer), do: :inet_tcp.getaddrs(address, timer)
  def getaddrs(address, :inet6, timer), do: :inet6_tcp.getaddrs(address, timer)
end
