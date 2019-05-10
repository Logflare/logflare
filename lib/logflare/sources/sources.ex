defmodule Logflare.Sources do
  alias Logflare.TableRateCounter, as: TRC

  def get_api_rate_by_id(sid, bucket: :default) do
    TRC.get_avg_rate(sid) 
  end
end
