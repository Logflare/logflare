defmodule Logflare.Sources do
  alias Logflare.SourceRateCounter, as: SRC

  def get_metrics(sid, bucket: :default) do
    SRC.get_metrics(sid, :default)
  end

  def get_api_rate_by_id(sid, bucket: :default) do
    SRC.get_avg_rate(sid)
  end
end
