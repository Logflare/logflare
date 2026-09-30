defmodule Logflare.Backends.Spool.HttpClient do
  @moduledoc false

  use Logflare.Networking.FinchHttpClient, finch_name: Logflare.FinchSpoolS3
end
