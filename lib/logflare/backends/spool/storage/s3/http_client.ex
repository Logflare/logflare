defmodule Logflare.Backends.Spool.Storage.S3.HttpClient do
  @moduledoc false

  use Logflare.Networking.FinchHttpClient, finch_name: Logflare.FinchSpoolS3
end
