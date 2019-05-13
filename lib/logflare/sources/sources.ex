defmodule Logflare.Sources do
  alias Logflare.{Repo, Source}
  alias Logflare.SourceRateCounter, as: SRC

  def get_metrics(sid, bucket: :default) do
    SRC.get_metrics(sid, :default)
  end

  def get_api_rate_by_id(sid, bucket: :default) do
    SRC.get_avg_rate(sid)
  end

  def get_by_id(source_id) when is_atom(source_id) do
    Source
    |> Repo.get_by(token: source_id)
    |> Repo.preload(:rules)
  end
end
