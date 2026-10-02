defmodule Logflare.Backends.Spool.Queue.SQS.HttpClient do
  @moduledoc false

  use Logflare.Networking.FinchHttpClient, finch_name: Logflare.FinchSpoolSQS
end
