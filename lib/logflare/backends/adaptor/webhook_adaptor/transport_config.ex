defmodule Logflare.Backends.Adaptor.WebhookAdaptor.TransportConfig do
  @moduledoc false

  @enforce_keys [:url, :headers, :format_batch, :gzip, :http]
  defstruct @enforce_keys
end
