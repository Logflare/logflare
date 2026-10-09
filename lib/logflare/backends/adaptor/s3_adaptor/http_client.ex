defmodule Logflare.Backends.Adaptor.S3Adaptor.HttpClient do
  @moduledoc false

  use Logflare.Networking.FinchHttpClient, finch_name: Logflare.FinchS3
end
